package spinal.lib.bus.tilelink.coherent

import spinal.core._
import spinal.lib._
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink._
import spinal.lib.misc.Plru
import spinal.lib.pipeline._

import scala.collection.mutable.ArrayBuffer

case class DirectoryParam(var unp : NodeParameters,
                          var downPendingMax : Int,
                          var cacheWays: Int,
                          var cacheBytes: Int,
                          var blockSize : Int,
                          var cacheBanks : Int = 1,
                          var probeCount : Int = 8,
                          var aBufferCount: Int = 4,
                          var ctrlLoopbackDepth : Int = 4,
                          var generalSlotCount : Int = 8,
                          var generalSlotCountUpCOnly : Int = 2,
                          var victimBufferLines : Int = 2,
                          var upCBufferDepth : Int = 8,
                          var probeRegion : UInt => Bool,
                          var allocateOnMiss : (Directory.CtrlOpcode.C, UInt, UInt, UInt) => Bool = null // opcode, source, address, size
                         ) {
  assert(isPow2(cacheBytes))

  def lockSets = cacheSets //TODO min trackedSets !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  def cacheLines = cacheBytes / blockSize
  def cacheSets = cacheLines / cacheWays
  def addressWidth = unp.m.addressWidth
  def dataWidth = unp.m.dataWidth
  def dataBytes = dataWidth/8
  def tagRange = addressWidth-1 downto log2Up(cacheBytes/cacheWays)
  def lineRange = tagRange.low-1 downto log2Up(lineSize)
  def wordRange = log2Up(lineSize)-1 downto log2Up(dataBytes)
  def wordsPerLine = lineSize/dataBytes
  def refillRange = tagRange.high downto lineRange.low
  def blockRange = addressWidth-1 downto log2Up(lineSize)
  def lineSize = blockSize
  def setsRange = lineRange
  def cacheAddressWidth = log2Up(cacheBytes/dataBytes)

  def addressCheckRange = setsRange.high downto log2Up(lineSize) //For now, it also avoid way clash (gsHits)
  def addressCheckWidth = addressCheckRange.size
//  def lockSetsRange = log2Up(lineSize*lockSets)-1 downto log2Up(lineSize)
}


object Directory{
  val CtrlOpcode = new SpinalEnum {
    val ACQUIRE_BLOCK, ACQUIRE_PERM, RELEASE, RELEASE_DATA, PUT_PARTIAL_DATA, PUT_FULL_DATA, GET, EVICT = newElement()
  }

  val ToUpDOpcode = new SpinalEnum {
    val NONE, ACCESS_ACK, ACCESS_ACK_DATA, GRANT, GRANT_DATA, RELEASE_ACK = newElement()
  }

  def downM2s(name : Nameable,
              addressWidth : Int,
              dataWidth : Int,
              blockSize : Int,
              generalSlotCount : Int) = M2sParameters(
    addressWidth = addressWidth,
    dataWidth = dataWidth,
    masters = List(M2sAgent(
      name = name,
      mapping = List(M2sSource(
        id = SizeMapping(0, 2 << log2Up(generalSlotCount)),
        emits = M2sTransfers(
          putPartial = SizeRange.upTo(blockSize),
          putFull = SizeRange.upTo(blockSize),
          get = SizeRange.upTo(blockSize)
        )
      )
      ))
    )
  )

  def upS2m(name : Nameable,
            blockSize : Int,
            generalSlotCount : Int) = S2mParameters(List(
    S2mAgent(
      name = name,
      emits = S2mTransfers(
        probe = SizeRange(blockSize)
      ),
      sinkId = SizeMapping(0, generalSlotCount)
    )
  ))
}


//class BankedMem[T <: Data](wordType: HardType[T], words: Int, banks : Int) extends Area{
//  def createWritePort(maskWidth : Int = -1): Stream[MemWriteCmd[T]] = {
//    ???
//  }
//
//  def createReadSyncPort(): MemReadStreamFlowPort[T] = {
//    ???
//  }
//
//  def build() = new Area{
//
//  }
//}



/*

get / put
- lock
- [probe victim]
- backend
- ack

acquire
- lock
- [probe hit]
- [update tags], [probe victim]
- backend
- ack

probeAck
- get context from probe slots
- [update tags]

probeAckData
- get context from probe slots
- [update tags], [probe victim]
- backend
- probes update

release
- [update tags] (as release -> probe can be out of order)
- ack

releaseData
- update tags, [probe victim]
- backend
- ack


Probes to deal with :
- All toN : from get / put / acquire / releaseData / probeData which need room
- Others toN : from acquire XtoT
- single TtoB : from acquire NtoB while line is in a unique state


get : [probe all toN] => read $/down, down may as well refill $
put : [probe all toN] => write down else refill cache then write it
acquire : [probe] => read $/down, down may as well refill $
upC data : write $/down => [wake probe]


Stuff which need concurrent protection :
- Cache line refill
- address inflight (to avoid read <> write coherency issue)
- address being probed (this will also ensure probe_ack_data are good)

Backend ops :
- up.a/c to $/down
- $ to up
- $ to victim [wait up.c], then write victim to down
- down to $/up [then write up.a to $]

down.D can trigger :
- to $
- to up.D
- up.a to $ (up.a.put allocating a cache line)
- if non inclusive cache : probe completion (initiated from probe_data_ack)


General slot allocations on :
- upA   : When it either directly go to backend or go to probe
- upC   : When release_data
 */

class Directory(val p : DirectoryParam) extends Component {
  import p._
  import Directory.CtrlOpcode

  assert(generalSlotCountUpCOnly < generalSlotCount)

  val ubp = p.unp.toBusParameter()
  val dbp = NodeParameters(
    m = Directory.downM2s(
      name = this,
      addressWidth = addressWidth,
      dataWidth = dataWidth,
      blockSize = blockSize,
      generalSlotCount = generalSlotCount
    ),
    s = S2mParameters.none()
  ).toBusParameter()

  val io = new Bundle {
    val up = slave(Bus(ubp))
    val down = master(Bus(dbp))
    val ordering = new Bundle {
      val ctrlProcess, writeBackend = master(Flow(OrderingCmd(up.p.sizeBytes)))
      def all = List(ctrlProcess, writeBackend)
    }
  }

  this.addTags(io.ordering.all.map(OrderingTag(_)))

  val coherentMasters = unp.m.masters.filter(_.emits.withBCE)
  val coherentMasterCount = coherentMasters.size
  val coherentMasterToSource = coherentMasters.map(_.bSourceId)

  val SET_ID = Stageable(UInt(log2Up(lockSets) bits))
  val LOCK_CTX = Stageable(Bool())
  val ADDRESS = Stageable(ubp.address)
  val GS_ID = Stageable(UInt(log2Up(generalSlotCount) bits))

  val emptyId = 0
  def isBlockEmpty(blockId : UInt) = blockId === emptyId
  def isBlockWithData(blockId : UInt) = blockId.msb

  val initializer = new Area {
    val initCycles = Math.max(p.lockSets, p.cacheWays)
    val counter = Reg(UInt(log2Up(initCycles) + 1 bits)) init (0)
    val done = counter.msb
    when(!done) {
      counter := counter + 1
    }
  }


  case class Tags(val withData : Boolean) extends Bundle {
    val loaded = Bool()
    val tag = UInt(tagRange.size bits)
    val dirty = withData generate Bool()
    val trunk = Bool()
    val owners = Bits(coherentMasterCount bits)
  }




  class LineCtrl(bytes : Int, ways: Int, withData : Boolean) extends Area {
    val sets = bytes / blockSize / ways
    val plru = new Area{
      val ram = Mem.fill(sets)(Plru.State(cacheWays))
      val read = ram.readSyncPort
      val write = ram.writePort
    }

    val tags = new Area {
      val ram = Mem.fill(sets)(Vec.fill(ways)(Tags(withData)))
      val read = ram.readSyncPort
      val writeRaw = ram.writePortWithMask(ways)
      val write = new Area{
        val valid = Bool()
        val address = ram.addressType()
        val mask = Bits(ways bits)
        val data = Tags(withData)

        writeRaw.valid := valid
        writeRaw.address := address
        writeRaw.mask := mask
        writeRaw.data.foreach(_:= data)

        assert(!(valid && data.trunk && data.owners === 0))
      }
    }
    val data = withData generate new Area{
      val upWrite, downWrite = Stream(MemWriteCmd(Bits(p.dataWidth bits), cacheAddressWidth, p.dataBytes))
      val upWriteDemux = StreamDemux(upWrite, upWrite.address.resize(log2Up(cacheBanks)), cacheBanks)
      val downWriteDemux = StreamDemux(downWrite, downWrite.address.resize(log2Up(cacheBanks)), cacheBanks)
      val read = Stream(UInt(cacheAddressWidth bits))
      val readIntend = Bool()

      val banks = for(i <- 0 until cacheBanks) yield new Area{
        val ram = Mem.fill(cacheBytes/p.dataBytes/cacheBanks)(Bits(p.dataWidth bits))
        val readed = Bits(p.dataWidth bits)
        val writeArbiter = StreamArbiterFactory().noLock.lowerFirst.buildOn(downWriteDemux(i), upWriteDemux(i))
        val write = writeArbiter.io.output.combStage()
      }

      val fpgaImpl = new Area{
        // Use simple dual port memories
        read.ready := True
        val b =  for((bank, i) <- banks.zipWithIndex) yield new Area{
          import bank._
          write.ready := True
          ram.write(write.address >> log2Up(cacheBanks), write.data, write.valid, write.mask)

          val readSel =  read.valid && read.payload.resize(log2Up(cacheBanks)) === i
          readed := ram.readSync(read.payload >> log2Up(cacheBanks), readSel)
        }
      }
    }
  }

  val cache = new LineCtrl(cacheBytes, cacheWays, true)



  val setsLock = new Area {
    class MemArea extends Area {
      val mem = Mem.fill(lockSets)(Bool())
      val write = mem.writePort()
      write.valid := !initializer.done
      write.address := initializer.counter.resized
      write.data := False
    }

    val target = new MemArea
    val hit = new MemArea {
//      val upE = Stream(write.payload)
//      val downD = Stream(write.payload)
//      val arbiter = StreamArbiterFactory().noLock.lowerFirst.onArgs(upE, downD).toFlow
//      when(initializer.done) {
//        write << arbiter
//      }
    }

    val unlockEvent = False
  }


  class CtrlCmd() extends Bundle {
    val opcode = CtrlOpcode()
    val args = Bits(1 bits)
    val address = ubp.address()
    val size = ubp.size()
    val source = ubp.source()
    val bufferAId = BUFFER_A_ID()
    val probed = Bool()
    val probedUnique = Bool()
    val gsId = GS_ID() //Only valid when probed
    val debugId = DebugId()
    val withDataUpC = Bool()
    val evictWay = UInt(log2Up(cacheWays) bits)

    def toTrunk = args(0)
    def toNone = args(0)
  }

  class ProberCmd() extends CtrlCmd {
    val mask = Bits(coherentMasterCount bits)
    val probeToN = Bool()
    val evictClean = Bool()
  }


  class ReadDownCmd() extends Bundle {
    val gsId = GS_ID()
    val address = ubp.address()
    val size = ubp.size()
  }

//  val VICTIME_ID = Stageable(UInt(-1 bits)) ;???

  class WriteBackendCmd() extends Bundle {
    val fromUpA = Bool()
    val fromUpC = Bool()
    val toDownA = Bool() //else to cache
    val toUpD = Directory.ToUpDOpcode()

    val toT = Bool()
    val source = ubp.source()
    val gsId = GS_ID()
    val partialUpA = Bool()
    val address = ubp.address()
    val size = ubp.size()
    val wayId = UInt(log2Up(cacheWays) bits)
    val bufferAId = BUFFER_A_ID()
    val evict = Bool()
    val debugId = DebugId()
  }

  class ReadBackendCmd() extends Bundle {
    val toUpD = Bool() // else to victim
    def toVictim = !toUpD
    val toWriteBackend = Bool()

    val gsId = GS_ID()
    val address = ubp.address()
    val size = ubp.size()
    val wayId = UInt(log2Up(cacheWays) bits)
//    val victimId = VICTIME_ID()
    val upD = new Bundle {
      val opcode = Opcode.D()
      val param  = Bits(3 bits)
      val source = ubp.source()
    }
  }


  val CTRL_CMD = Stageable(new CtrlCmd())
  val BUFFER_A_ID = Stageable(UInt(log2Up(aBufferCount) bits))



  class Slot extends Area{
    val fire = False
    val valid = RegInit(False) clearWhen(fire)
  }
  class SlotPool[T <: Slot](slotsCount : Int)(gen : => T) extends Area{
    val slots = for(i <- 0 until slotsCount) yield gen
    val allocate = new Area{
      val full = slots.map(_.valid).andR
      val oh = B(OHMasking.firstV2(Vec(slots.map(!_.valid))))
      val id = OHToUInt(oh)
    }
  }

  // release_data =>
  // get => read, [lock], [victim]
  // put => write, [lock], [victim]
  // acquire_block => [lock], [victim]
  class GeneralSlot extends Slot{
    val address = Reg(UInt(addressCheckWidth bits))
    val way = Reg(UInt(log2Up(cacheWays) bits))
    val pending = new Area{
      val victim, primary, upC, victimRead = Reg(Bool()) //TODO upC ???
      fire setWhen(List(victim, primary, upC, victimRead).norR)

    }
  }

  case class CtxDownD() extends Bundle {
    val toUpD, toCache = Bool()
    val toProbe, mergeBufferA = Bool()
//    val probeId = UInt(log2Up(probeCount) bits)
    val bufferAId = BUFFER_A_ID()
    val wordOffset = UInt(wordRange.size bits)
    val sourceId = io.up.p.source()
    val setId = UInt(setsRange.size bits)
    val wayId = UInt(log2Up(cacheWays) bits)
    val size = ubp.size()
    val acquire = Bool()
    val toT = Bool()
  }

  val gs = new SlotPool(generalSlotCount)(new GeneralSlot){
    val ctxDownD = new Area{
      val ram = Mem.fill(generalSlotCount)(CtxDownD())
      val write = ram.writePort()
    }
    val fullUpA = slots.dropRight(p.generalSlotCountUpCOnly).map(_.valid).andR
  }



  val bufferA = new ChannelDataBuffer(
    entries = aBufferCount,
    blockSize = blockSize,
    dataBytes = ubp.dataBytes
  ){
    val read = ram.readSyncPort()

    val pusher = push(io.up.a.haltWhen(!initializer.done))
    val toCtrl = pusher.down.swapPayload(new CtrlCmd())
    toCtrl.bufferAId := pusher.bufferId
    toCtrl.opcode := pusher.down.opcode.muxDc(
      Opcode.A.GET -> CtrlOpcode.GET(),
      Opcode.A.PUT_PARTIAL_DATA -> CtrlOpcode.PUT_PARTIAL_DATA(),
      Opcode.A.PUT_FULL_DATA -> CtrlOpcode.PUT_FULL_DATA(),
      Opcode.A.ACQUIRE_PERM -> CtrlOpcode.ACQUIRE_PERM(),
      Opcode.A.ACQUIRE_BLOCK -> CtrlOpcode.ACQUIRE_BLOCK()
    )
    toCtrl.toTrunk := pusher.down.param =/= Param.Grow.NtoB
    toCtrl.address := pusher.down.address
    toCtrl.size := pusher.down.size
    toCtrl.source := pusher.down.source
    toCtrl.probed := False
    toCtrl.gsId.assignDontCare()
    toCtrl.debugId := pusher.down.debugId
    toCtrl.withDataUpC := False
    toCtrl.evictWay.assignDontCare()
    toCtrl.probedUnique := False
  }



  val victimBuffer = new Area{
    val ram = Mem.fill(generalSlotCount*wordsPerLine)(io.up.p.data())
    val write = ram.writePort()
    val read = ram.readSyncPort()
  }


  val upCSplit = new Area{
    val (cmdFork, dataFork) = StreamFork2(io.up.c)
    val cmd = cmdFork.translateWith(io.up.c.asNoData()).takeWhen(io.up.c.isFirst())
    val data = dataFork.takeWhen(io.up.c.withBeats).translateWith(dataFork.data)
    val dataPop = data.queue(upCBufferDepth).m2sPipe()
  }

  class ProberSlot extends Slot{
    val address = Reg(UInt(addressCheckWidth bits))
    val pending = Reg(UInt(log2Up(coherentMasterCount + 1) bits))
    val probeAckDataCompleted = Reg(Bool())
    val unique = Reg(Bool())
    val done = valid && (pending & ~U(probeAckDataCompleted, widthOf(pending) bits)) === 0
    val evictClean = Reg(Bool())
  }

  //Currently we ignore the cache owners tracking of PROBE_ACK_DATA TtoN (will behave like a TtoB)
  val prober = new SlotPool(probeCount)(new ProberSlot){
    val ctx = new Area{
      val ram = Mem.fill(probeCount)(new CtrlCmd())
      val write = ram.writePort()
    }

    val cmd = Stream(new ProberCmd())

    val upB = new Area{
      val push = cmd.m2sPipe()
      val sloted = RegInit(False)
      val full = slots.map(_.valid).andR
      val pending = CountOne(push.mask)

      ctx.write.valid := False
      ctx.write.address := allocate.id
      ctx.write.data.assignSomeByName(push.payload)

      when(push.valid && !sloted && !allocate.full) {
        slots.onMask(allocate.oh) { s =>
          s.valid := True
          s.address := push.address(addressCheckRange)
          s.pending := pending
          s.unique := True
          s.probeAckDataCompleted := False
          s.evictClean := push.evictClean
        }
        ctx.write.valid := True
        sloted := True
      }

      val bus = io.up.b
      val halted = push.haltWhen(!sloted && full)
      val fired = Reg(Bits(coherentMasterCount bits)) init (0)
      val requests = push.mask & ~fired
      val masterOh = OHMasking.firstV2(requests)
      bus.valid := halted.valid && requests.orR
      bus.opcode := Opcode.B.PROBE_BLOCK
      bus.param := halted.probeToN ? B(Param.Cap.toN, 3 bits) | B(Param.Cap.toB, 3 bits)
      bus.source := OhMux(masterOh, coherentMasterToSource.map(id => U(id, ubp.sourceWidth bits)).toList)
      bus.address := halted.address(blockRange) @@ U(0, blockRange.low bits)
      bus.size := log2Up(p.blockSize)
      halted.ready := requests === 0

      when(bus.fire) {
        fired.asBools.onMask(masterOh)(_ := True)
      }

      when(halted.ready) {
        fired := 0
        sloted := False
      }
    }

    val upC = new Area {
      val input = upCSplit.cmd
      val isProbeNoData = input.opcode === Opcode.C.PROBE_ACK
      val isProbe = Opcode.C.isProbe(input.opcode)
      val hitOh = slots.map(s => s.valid && s.address === input.address(addressCheckRange))
      val hitId = OHToUInt(hitOh)
      val pending = slots.map(_.pending).read(hitId)
      val pendingNext = pending - 1
      val masterOh = coherentMasterToSource.map(input.source === _).asBits
      val masterId = OHToUInt(masterOh)
      val keptCopy = isProbe && Param.reportPruneKeepCopy(input.param)

      when(input.fire) {
        slots.onSel(hitId) { s =>
          s.unique.clearWhen(keptCopy)
          when(isProbeNoData) {
            s.pending := pendingNext
          }
        }
      }

      val filtred = input.throwWhen(upCSplit.cmd.opcode === Opcode.C.PROBE_ACK)
      class UpCCmd extends Bundle{
        val hitId = UInt(log2Up(probeCount) bits)
        val opcode = Opcode.C()
        val address = ubp.address()
        val source  = ubp.source()
        val toNone = Bool()
      }
      val down = filtred.swapPayload(new UpCCmd)
      down.hitId   := hitId
      down.opcode  := input.opcode
      down.address := input.address
      down.source  := input.source
      down.toNone  := !Param.reportPruneKeepCopy(input.param)
    }

    val schedule = new Area {
      val fromUpC = upC.down.halfPipe()
      val fromProbe = Stream(NoData())
      val merged = Stream(new CtrlCmd())
      val hits = slots.map(_.done).asBits
      val hitOh = CombInit(OHMasking.roundRobinNext(hits, fromProbe.fire))
      val probeId = OHToUInt(hitOh)

      fromProbe.valid := hits.orR
      when(fromProbe.fire || fromUpC.fire && fromUpC.opcode === Opcode.C.PROBE_ACK_DATA) {
        slots.onSel(probeId) { s =>
          s.fire := True
        }
      }

      //fromUpC does not come for PROBE_ACK
      fromUpC.ready := merged.ready
      fromProbe.ready := merged.ready && !fromUpC.valid
      merged.valid := fromUpC.valid || fromProbe.valid
      merged.payload := ctx.ram.readAsync(probeId)
      merged.probedUnique.removeAssignments() := slots.reader(hitOh)(_.unique)
      when(fromUpC.valid) {
        probeId := fromUpC.hitId
        merged.probed clearWhen(Opcode.C.isRelease(fromUpC.opcode))
        merged.withDataUpC setWhen(Opcode.C.withData(fromUpC.opcode))
        merged.probedUnique := fromUpC.toNone
        when(Opcode.C.isRelease(fromUpC.opcode)){
          merged.address := fromUpC.address
          merged.source := fromUpC.source
          merged.toNone := fromUpC.toNone
          merged.opcode := fromUpC.opcode.muxDc(
            Opcode.C.RELEASE -> CtrlOpcode.RELEASE(),
            Opcode.C.RELEASE_DATA -> CtrlOpcode.RELEASE_DATA()
          )
        }
      }

      val isEvict = merged.opcode === CtrlOpcode.EVICT
      val (toCtrl, toEvict) = StreamDemux.two(merged, isEvict)

      val isEvictClean = !fromUpC.valid && (hitOh & slots.map(_.evictClean).asBits).orR
      val toWriteBackend = toEvict.throwWhen(isEvictClean).swapPayload(new WriteBackendCmd())
      toWriteBackend.fromUpA := False
      toWriteBackend.fromUpC := merged.withDataUpC
      toWriteBackend.toDownA := True
      toWriteBackend.toUpD := Directory.ToUpDOpcode.NONE
      toWriteBackend.toT := True
      toWriteBackend.source := 0
      toWriteBackend.gsId := toEvict.gsId
      toWriteBackend.partialUpA := False
      toWriteBackend.address := toEvict.address
      toWriteBackend.size := log2Up(blockSize)
      toWriteBackend.wayId := 0
      toWriteBackend.bufferAId := 0
      toWriteBackend.evict := True
      toWriteBackend.debugId := 0

      when(merged.valid && isEvictClean){
        gs.slots.onSel(merged.gsId){s =>
          s.pending.victim := False
        }
      }
    }
  }

  //TODO check older way is allocated
  val ctrl = new Pipeline{
    val stages = newChained(3, Connection.M2S())
    val inserterStage = stages(0)
    val addressStage = stages(0)
    val dataStage = stages(1)
    val tagStage = stages(1)
    val prepStage = stages(1)
    val processStage = stages(2)

    import CtrlOpcode._


    val loopback = new Area{
      val occupancy = new CounterUpDown(ctrlLoopbackDepth, handleOverflow = false)
      val allowUpA = !occupancy.mayOverflow
      val fifo = StreamFifo(new CtrlCmd, ctrlLoopbackDepth, forFMax = true)
    }

    val inserter = new Area {
      import inserterStage._

      def hazardHalt(that : Stream[CtrlCmd]) = {
        val hits = stages.tail.map(s => s.valid && s(CTRL_CMD).address(setsRange) === that.address(setsRange))
        that.haltWhen(hits.orR)
      }

      val arbiter = StreamArbiterFactory().lowerFirst.noLock.build(CTRL_CMD, 3)
      arbiter.io.inputs(0) << prober.schedule.toCtrl.pipelined(m2s = true, s2m = true)
      arbiter.io.inputs(1) << loopback.fifo.io.pop.halfPipe()
      arbiter.io.inputs(2) << bufferA.toCtrl.continueWhen(loopback.allowUpA)

      when(bufferA.toCtrl.fire) {
        loopback.occupancy.increment()
      }

      driveFrom(hazardHalt(arbiter.io.output))
      inserterStage(CTRL_CMD) := arbiter.io.output

      val SOURCE_OH = insert(B(coherentMasters.map(_.sourceHit(CTRL_CMD.source))))
    }

    cache.tags.read.cmd.valid := addressStage.isFireing
    cache.tags.read.cmd.payload := addressStage(CTRL_CMD).address(lineRange)
    val CACHE_TAGS = dataStage.insert(cache.tags.read.rsp)

    cache.plru.read.cmd.valid := addressStage.isFireing
    cache.plru.read.cmd.payload := addressStage(CTRL_CMD).address(setsRange)
    val CACHE_PLRU = dataStage.insert(cache.plru.read.rsp)


    val tags = new Area{
      import tagStage._
      val read = tagStage(CACHE_TAGS)
      val CACHE_HITS  = insert(read.map(t => t.loaded && t.tag === CTRL_CMD.address(tagRange)).asBits)
      val SOURCE_HITS = insert(read.map(t => (t.owners & inserter.SOURCE_OH).orR).asBits)
    }

    val preCtrl = new Area{
      import prepStage._
      val ALLOCATE_ON_MISS = insert(p.allocateOnMiss(CTRL_CMD.opcode, CTRL_CMD.source, CTRL_CMD.address, CTRL_CMD.size)) //TODO
      val FROM_A = insert(List(GET(), PUT_FULL_DATA(), PUT_PARTIAL_DATA(), ACQUIRE_BLOCK(), ACQUIRE_PERM()).sContains(CTRL_CMD.opcode))
      val FROM_C_RELEASE = insert(List(RELEASE(), RELEASE_DATA()).sContains(CTRL_CMD.opcode))
      val GET_PUT = insert(List(GET(), PUT_FULL_DATA(), PUT_PARTIAL_DATA()).sContains(CTRL_CMD.opcode))
      val ACQUIRE = insert(List(ACQUIRE_PERM(), ACQUIRE_BLOCK()).sContains(CTRL_CMD.opcode))
      val IS_GET = insert(List(GET()).sContains(CTRL_CMD.opcode))
      val IS_PUT = insert(List(PUT_FULL_DATA(), PUT_PARTIAL_DATA()).sContains(CTRL_CMD.opcode))
      val IS_PUT_FULL_BLOCK = insert(CTRL_CMD.opcode === CtrlOpcode.PUT_FULL_DATA && CTRL_CMD.size === log2Up(blockSize))
      val WRITE_DATA = insert(List(PUT_PARTIAL_DATA(), PUT_FULL_DATA(), RELEASE_DATA()).sContains(CTRL_CMD.opcode))
      val GS_NEED = insert(List(ACQUIRE_BLOCK, ACQUIRE_PERM, RELEASE_DATA, PUT_PARTIAL_DATA, PUT_FULL_DATA, GET).map(_.craft()).sContains(CTRL_CMD.opcode))
      val gsHits = gs.slots.map(s => s.valid && CTRL_CMD.address(addressCheckRange) === s.address)
      val GS_HIT = insert(gsHits.orR)
      val GS_OH = insert(UIntToOh(CTRL_CMD.gsId))

      //For as long as the cache is inclusive
      when(ACQUIRE){
        ALLOCATE_ON_MISS := True
      }
    }

    val process = new Area{
      import processStage._

      val firstCycle = RegNext(!isStuck) init(True)

      val redoUpA = False
      assert(!(isValid && redoUpA && !preCtrl.FROM_A))
      throwIt(redoUpA)

      redoUpA.setWhen(preCtrl.FROM_A && !CTRL_CMD.probed && preCtrl.GS_HIT) //TODO could be less pessimistic


      val stallIt = False
      assert(!(isValid && stallIt && preCtrl.FROM_A))
      haltIt(stallIt)

      val askAllocate = False //Will handle victim
      val askProbe = False
      val askReadDown = False
      val askReadBackend = False
      val askOrdering = False
      val askWriteBackend = False
      val askGs = preCtrl.GS_NEED && !CTRL_CMD.probed
      val askUpD = False

      when(askGs){
        when(preCtrl.FROM_A && gs.fullUpA){
          redoUpA := True
        }
        when(!preCtrl.FROM_A && gs.allocate.full){
          stallIt := True
        }
      }

      when(isFireing && preCtrl.FROM_A && askGs && !redoUpA){
        loopback.occupancy.decrement()
      }

//      val toProbe = forkStream(askProbe && !redoUpA).swapPayload(new ProbeCmd())
      val toReadDown = forkStream(askReadDown && !redoUpA).swapPayload(new ReadDownCmd)
      val toReadBackend = forkStream(askReadBackend && !redoUpA).swapPayload(new ReadBackendCmd)
      val toWriteBackend = forkStream(askWriteBackend && !redoUpA && !stallIt).swapPayload(new WriteBackendCmd)
      val toUpD = forkStream(askUpD && !redoUpA).swapPayload(io.up.d.payloadType)
      val toOrdering = forkFlow(askOrdering && !redoUpA).swapPayload(io.ordering.ctrlProcess.payloadType)

      val CACHE_HIT = insert(tags.CACHE_HITS.orR)
      val CACHE_HIT_WAY_ID = insert(OHToUInt(tags.CACHE_HITS))
      val SOURCE_HIT = insert((tags.CACHE_HITS & tags.SOURCE_HITS).orR)
      val CACHE_LINE = insert(OhMux.or(tags.CACHE_HITS, CACHE_TAGS))
      val OTHERS = insert(CACHE_LINE.owners & ~inserter.SOURCE_OH)
      val SELF = insert((CACHE_LINE.owners & inserter.SOURCE_OH).orR)
      val OTHER = insert(OTHERS.orR)
      val ANY = insert(CACHE_LINE.owners.orR)

      val backendWayId = CACHE_HIT_WAY_ID()

      val gotGs = RegInit(False)
      val gsOhLocked = RegNextWhen(gs.allocate.oh, !gotGs)
      val gsOh = gotGs.mux(gsOhLocked, gs.allocate.oh)
      when(CTRL_CMD.probed) {
        gsOh := preCtrl.GS_OH
      }

      val gsId = OHToUInt(gsOh)
      val gsAddress = CombInit(CTRL_CMD.address(addressCheckRange))
      val gsRefill = False
      val gsWrite = preCtrl.WRITE_DATA || CTRL_CMD.withDataUpC
      val gsWay = CombInit(backendWayId)
      val gsPendingVictim = False
      val gsPendingVictimRead = False
      val gsPendingPrimary = True
      val gsPendingUpc = CTRL_CMD.withDataUpC

      //TODO don't forget to ensure that a victim get out of the cache before downD/upA erase it

      toUpD.opcode.assignDontCare()
      toUpD.source  := CTRL_CMD.source
      toUpD.sink    := gsId
      toUpD.size    := log2Up(blockSize)
      toUpD.param   := 0
      toUpD.denied  := False
      toUpD.corrupt := False
      toUpD.data.assignDontCare()


      prober.cmd.valid := isValid && askProbe && !redoUpA && firstCycle
      prober.cmd.payload.assignSomeByName(CTRL_CMD)
      prober.cmd.mask.assignDontCare()
      prober.cmd.probeToN.assignDontCare()
      prober.cmd.probed.removeAssignments() := True
      prober.cmd.gsId.removeAssignments()    := gsId
      prober.cmd.evictClean := False
      prober.cmd.evictWay.removeAssignments() := backendWayId

      loopback.fifo.io.push.valid := isValid && redoUpA
      loopback.fifo.io.push.payload := CTRL_CMD
      redoUpA setWhen(!CTRL_CMD.probed && preCtrl.FROM_A && !prober.cmd.ready && firstCycle)
      assert(!(isValid && redoUpA && !loopback.fifo.io.push.ready))

      val doIt = isFireing && !isRemoved

      val olderWay = new Area{
        val plru = new Plru(cacheWays, false)
        plru.io.context.state := CACHE_PLRU

        cache.plru.write.valid := CACHE_HIT
        plru.io.update.id := CACHE_HIT_WAY_ID

        when(askAllocate) {
          cache.plru.write.valid := True
          plru.io.update.id := plru.io.evict.id
        }

        cache.plru.write.valid clearWhen (!isFireing)
        cache.plru.write.address := CTRL_CMD.address(setsRange)
        cache.plru.write.data := plru.io.update.state

        val unlocked = CombInit(!preCtrl.GS_HIT) //Pessimistic, as way check could help reduce conflict
        val wayId = CombInit(plru.io.evict.id)
        val tags = CACHE_TAGS(wayId)
        val address = tags.tag @@ CTRL_CMD.address(setsRange) @@ U(0, log2Up(blockSize) bits)
      }

      backendWayId := CACHE_HIT_WAY_ID | olderWay.wayId.andMask(askAllocate)

      cache.tags.write.valid := tags.CACHE_HITS.orR || askAllocate
      cache.tags.write.address := CTRL_CMD.address(setsRange)
      cache.tags.write.mask := tags.CACHE_HITS | UIntToOh(olderWay.wayId).andMask(askAllocate)
      cache.tags.write.data.loaded := True
      cache.tags.write.data.tag := CTRL_CMD.address(tagRange)
      cache.tags.write.data.dirty := CACHE_LINE.dirty && !askAllocate
      cache.tags.write.data.trunk := CACHE_LINE.trunk

      val owners = new Area {
        val add, remove, clean = False
        val next = (CACHE_LINE.owners.andMask(!clean) | inserter.SOURCE_OH.andMask(add)) & ~inserter.SOURCE_OH.andMask(remove)
        cache.tags.write.data.owners.removeAssignments() := next

        clean setWhen(CTRL_CMD.probed && CTRL_CMD.probedUnique)
      }


      when(isValid && askGs && !redoUpA && !gs.allocate.full) {
        gotGs := True
        gs.slots.onMask(gsOh) { s =>
          s.address := gsAddress
          s.way :=  gsWay
          s.pending.victim := gsPendingVictim
          s.pending.victimRead := gsPendingVictimRead
          s.pending.primary := gsPendingPrimary
          s.pending.upC := gsPendingUpc

          when(isReady) {
            s.valid := True
          }
        }
      }
      gotGs clearWhen (isFireing)

      toReadBackend.address := CTRL_CMD.address
      toReadBackend.size := CTRL_CMD.size
      toReadBackend.gsId := gsId
      toReadBackend.wayId := backendWayId
      toReadBackend.toUpD := False
      toReadBackend.upD.opcode.assignDontCare()
      toReadBackend.upD.param := 0
      toReadBackend.upD.source := CTRL_CMD.source
      toReadBackend.toWriteBackend := False

      toWriteBackend.toDownA    := False
      toWriteBackend.fromUpA    := preCtrl.IS_PUT
      toWriteBackend.fromUpC    := CTRL_CMD.withDataUpC
      toWriteBackend.address    := CTRL_CMD.address
      toWriteBackend.size       := CTRL_CMD.size
      toWriteBackend.gsId       := gsId
      toWriteBackend.bufferAId  := CTRL_CMD.bufferAId
      toWriteBackend.partialUpA := CTRL_CMD.opcode === CtrlOpcode.PUT_PARTIAL_DATA
      toWriteBackend.wayId      := backendWayId
      toWriteBackend.source     := CTRL_CMD.source
      toWriteBackend.toT        := True
      toWriteBackend.toUpD      := Directory.ToUpDOpcode.NONE()
      toWriteBackend.evict      := False
      toWriteBackend.debugId    := CTRL_CMD.debugId

      toReadDown.gsId    := gsId
      toReadDown.address := CTRL_CMD.address
      toReadDown.size    := CTRL_CMD.size
      when(preCtrl.ALLOCATE_ON_MISS){
        toReadDown.address(log2Up(blockSize)-1 downto 0) := 0
        toReadDown.size := log2Up(blockSize)
      }

      val ctxDownDWritten = RegInit(False) setWhen (gs.ctxDownD.write.valid) clearWhen (isFireing)
      val ctxDownD = gs.ctxDownD.write
      ctxDownD.valid := isValid && askGs && !redoUpA && !stallIt && !ctxDownDWritten
      ctxDownD.address            := gsId
      ctxDownD.data.toUpD         := False
      ctxDownD.data.toCache       := False
      ctxDownD.data.toProbe       := False
      ctxDownD.data.toT           := True
      ctxDownD.data.acquire       := preCtrl.ACQUIRE
      ctxDownD.data.mergeBufferA  := False
      ctxDownD.data.bufferAId     := CTRL_CMD.bufferAId
      ctxDownD.data.wordOffset    := CTRL_CMD.address(wordRange)
      ctxDownD.data.setId         := CTRL_CMD.address(setsRange)
      ctxDownD.data.size          := CTRL_CMD.size
      ctxDownD.data.sourceId      := CTRL_CMD.source
      ctxDownD.data.wayId         := backendWayId

      toOrdering.debugId := CTRL_CMD.debugId
      toOrdering.bytes := (U(1) << CTRL_CMD.size).resized
      toOrdering >> io.ordering.ctrlProcess

      //Generate a victim
      when(askAllocate && olderWay.tags.loaded){
        when(olderWay.tags.owners.orR) {
          askProbe := True
        } otherwise {
          toReadBackend.toWriteBackend := True
        }

        when(olderWay.tags.dirty || olderWay.tags.trunk) {
          askReadBackend := True
          gsPendingVictim := True
          gsPendingVictimRead := True
        }

        toReadBackend.address   := olderWay.address
        toReadBackend.size      := log2Up(blockSize)
        toReadBackend.wayId  := olderWay.wayId

        prober.cmd.opcode := CtrlOpcode.EVICT
        prober.cmd.address := olderWay.address
        prober.cmd.size := log2Up(blockSize)
        prober.cmd.mask := olderWay.tags.owners
        prober.cmd.probeToN := True
        prober.cmd.evictClean := !olderWay.tags.dirty


        when(!olderWay.unlocked){
          //Assume it come from A (inclusive)
          assert(!isValid || preCtrl.FROM_A)
          redoUpA := True
        }
      }

      assert(!(isValid && CTRL_CMD.probed && askAllocate))

      when(preCtrl.FROM_C_RELEASE){
        //Update tags
        owners.remove setWhen (CTRL_CMD.toNone)
        cache.tags.write.data.trunk := False

        when(valid){
          cache.tags.write.valid := True
          assert(CACHE_HIT)  //As it is a inclusive cache
        }

        //Write to backend
        when(preCtrl.WRITE_DATA){
          askGs := True
          askWriteBackend := True
          toWriteBackend.toUpD := Directory.ToUpDOpcode.RELEASE_ACK()
          cache.tags.write.data.dirty := True
          gsWrite := True
        } otherwise {
          askUpD := True
          toUpD.opcode := Opcode.D.RELEASE_ACK
        }
      }

      val getPutNeedProbe = CACHE_HIT && ANY && !CTRL_CMD.probed && (!preCtrl.IS_GET || CACHE_LINE.trunk)
      when(preCtrl.GET_PUT){
        gsWrite := !preCtrl.IS_GET
        owners.clean setWhen(preCtrl.IS_PUT)
        cache.tags.write.data.trunk := False

        //Ensure that the cache.others is cleared on PUT
        when(CACHE_HIT){
          cache.tags.write.valid := True
        }

        when(getPutNeedProbe){
          askProbe := True
          prober.cmd.mask := CACHE_LINE.owners
          prober.cmd.probeToN := !preCtrl.IS_GET
          //TODO ensure that once the probe is done, the initial request isn't overtaken by another one (ex acquire)
        } otherwise {
          when(CACHE_HIT) {
            when(CTRL_CMD.withDataUpC){
              askWriteBackend := True
              cache.tags.write.data.dirty := True
            } otherwise {
              askOrdering := True
              askReadBackend := preCtrl.IS_GET
              toReadBackend.toUpD := True
              toReadBackend.upD.opcode := Opcode.D.ACCESS_ACK_DATA
              when(preCtrl.IS_PUT) {
                askWriteBackend := True
                cache.tags.write.data.dirty := True
              }
            }
            toWriteBackend.toUpD := preCtrl.IS_GET.mux(
              Directory.ToUpDOpcode.ACCESS_ACK_DATA(),
              Directory.ToUpDOpcode.ACCESS_ACK()
            )
          }.elsewhen(preCtrl.ALLOCATE_ON_MISS) {
            askOrdering := True
            askAllocate := True
            ctxDownD.data.toCache := True
            when(preCtrl.IS_PUT_FULL_BLOCK){
              askWriteBackend := True
              toWriteBackend.toUpD := Directory.ToUpDOpcode.ACCESS_ACK
            } otherwise {
              askReadDown := True
            }
            gsRefill := True
            when(preCtrl.IS_GET) {
              ctxDownD.data.toUpD := True
            }
            when(preCtrl.IS_PUT) {
              ctxDownD.data.mergeBufferA := True
              cache.tags.write.data.dirty := True
            }
          }.otherwise {
            askOrdering := True
            askReadDown := preCtrl.IS_GET
            toWriteBackend.toDownA := True
            askWriteBackend := !preCtrl.IS_GET
            ctxDownD.data.toUpD := True
          }
        }
      }

      val aquireToB = !CTRL_CMD.toTrunk && OTHER
      val acquireParam = aquireToB.mux[Bits](Param.Cap.toB, Param.Cap.toT)


      when(preCtrl.ACQUIRE){
        when(!CACHE_HIT){
          owners.clean := True
          owners.add := True
          askOrdering := True
          askAllocate := True
          ctxDownD.data.toUpD := True
          ctxDownD.data.toCache := True
          ctxDownD.data.toT := !aquireToB
          cache.tags.write.data.trunk := !aquireToB
          when(CTRL_CMD.opcode === CtrlOpcode.ACQUIRE_BLOCK) {
            askReadDown := True
            gsRefill := True
          } otherwise {
            askUpD := True
            toUpD.opcode := Opcode.D.GRANT
          }
        }otherwise{
          //Need probing ?
          when(!CTRL_CMD.probed && (CACHE_LINE.trunk || CTRL_CMD.toTrunk && OTHER)) {
            askProbe := True
            prober.cmd.mask := OTHERS
            prober.cmd.probeToN := CTRL_CMD.toTrunk
          } otherwise {
            when(aquireToB) {
              ctxDownD.data.toT := False
            } otherwise {
              owners.clean := True
            }
            owners.add := True
            cache.tags.write.data.trunk := !aquireToB

            //TODO warning gs may will complet before writebackend is done !
            when(CTRL_CMD.withDataUpC) {
              askWriteBackend := True
              cache.tags.write.data.dirty := True
            }

            when(CTRL_CMD.opcode === CtrlOpcode.ACQUIRE_BLOCK && !SELF) {
              toReadBackend.toUpD := True
              toReadBackend.upD.opcode := Opcode.D.GRANT_DATA
              toReadBackend.upD.param := acquireParam.resized

              toWriteBackend.toT := !aquireToB
              toWriteBackend.toUpD := Directory.ToUpDOpcode.GRANT_DATA()

              when(!CTRL_CMD.withDataUpC){
                askOrdering := True
                askReadBackend := True
              }
            } otherwise {
              askOrdering := True
              askUpD := True
              toUpD.opcode := Opcode.D.GRANT
              toUpD.param := acquireParam.resized
            }
          }
        }
      }

      when(!doIt){
        cache.tags.write.valid := False
      }
    }
  }

  val readDown = new Area {
    val cmd = ctrl.process.toReadDown.pipelined(m2s = true, s2m = true)
    val toDownA = cmd.swapPayload(io.down.a.payloadType())

    toDownA.opcode := Opcode.A.GET
    toDownA.param := 0
    toDownA.source := U"0" @@ cmd.gsId
    toDownA.address := cmd.address
    toDownA.size := cmd.size
    toDownA.mask.assignDontCare()
    toDownA.data.assignDontCare()
    toDownA.corrupt := False
    toDownA.debugId := DebugId.withPostfix(toDownA.source)
  }

  case class PutMergeCmd() extends Bundle{
    val gsId = GS_ID()
    val setId = UInt(setsRange.size bits)
    val wayId = UInt(log2Up(cacheWays) bits)
    val wordOffset = UInt(wordRange.size bits)
    val bufferAId = BUFFER_A_ID()
    val source = ubp.source()
    val size = ubp.size()
  }

  val readBackend = new Pipeline {
    val stages = newChained(3, Connection.M2S())
    val inserterStage = stages(0)
    val fetchStage = stages(0)
    val readStage = stages(1)
    val processStage = stages(2)

    val CMD = Stageable(new ReadBackendCmd())


    val inserter = new Area {

      import inserterStage._

      val cmd = ctrl.process.toReadBackend.pipelined(m2s = true, s2m = true)
      val counter = Reg(io.up.p.beat()) init (0)
      val LAST = insert(counter === sizeToBeatMinusOne(io.up.p, cmd.size))

      cmd.ready := isReady && LAST
      valid := cmd.valid
      inserterStage(CMD) := cmd.payload
      CMD.address.removeAssignments() := cmd.address | (counter << log2Up(p.dataBytes)).resized

      when(isFireing) {
        counter := counter + 1
        when(LAST) {
          counter := 0
        }
      }
    }

    val fetcher = new Area {

      import fetchStage._

      cache.data.readIntend := isValid
      cache.data.read.valid := isFireing
      cache.data.read.payload := CMD.wayId @@ CMD.address(setsRange.high downto wordRange.low)
    }

    val CACHED = readStage.insert(Vec(cache.data.banks.map(_.readed))) //May want KEEP attribute

    val process = new Area {

      import processStage._

      val DATA = insert(CACHED(CMD.address(log2Up(p.dataBytes), log2Up(cacheBanks) bits)))

      val toUpDFork = forkStream(CMD.toUpD)
      val toUpD = toUpDFork swapPayload io.up.d.payloadType()
      toUpD.opcode := CMD.upD.opcode
      toUpD.param := CMD.upD.param
      toUpD.source := CMD.upD.source
      toUpD.sink := CMD.gsId
      toUpD.size := CMD.size
      toUpD.denied := False
      toUpD.data := DATA
      toUpD.corrupt := False


      val toVictimFork = forkFlow(!CMD.toUpD)
      victimBuffer.write.valid := toVictimFork.valid
      victimBuffer.write.address := CMD.gsId @@ CMD.address(wordRange)
      victimBuffer.write.data := DATA

      val victimCounter = Reg(ubp.beat) init(0)
      val victimBusy = victimCounter =/= 0
      val victimGsId = CMD.gsId
      when(victimBuffer.write.valid){
        victimCounter := victimCounter + 1
      }

      val gsOh = UIntToOh(CMD.gsId)
      when(isFireing && inserter.LAST) {
        when(CMD.toUpD && Opcode.D.isFinal(CMD.upD.opcode)) {
          gs.slots.onMask(gsOh)(_.pending.primary := False)
        }
      }
      when(isFireing && CMD.toVictim && !victimBusy) {
        gs.slots.onMask(gsOh) { s =>
          s.pending.victimRead := False
        }
      }

      val toWriteBackendFork = forkStream(CMD.toWriteBackend && CMD.address(wordRange) === 0)
      val toWriteBackend = toWriteBackendFork.swapPayload(new WriteBackendCmd())
      toWriteBackend.fromUpA    := False
      toWriteBackend.fromUpC    := False
      toWriteBackend.toDownA    := True
      toWriteBackend.toUpD      := Directory.ToUpDOpcode.NONE
      toWriteBackend.toT        := True
      toWriteBackend.source     := 0
      toWriteBackend.gsId       := CMD.gsId
      toWriteBackend.partialUpA := False
      toWriteBackend.address    := CMD.address
      toWriteBackend.size       := log2Up(blockSize)
      toWriteBackend.wayId      := 0
      toWriteBackend.bufferAId  := 0
      toWriteBackend.evict      := True
      toWriteBackend.debugId    := 0
    }
  }

  val writeBackend = new Pipeline {
    val stages = newChained(3, Connection.M2S())
    val inserterStage = stages(0)
    val fetchStage = stages(0)
    val readStage = stages(1)
    val processStage = stages(2)

    val CMD = Stageable(new WriteBackendCmd())

    // Put merge is used on upA put which trigger a cache line load
    val putMerges = new Area {
      val push = Stream(PutMergeCmd())
      val fifo = StreamFifo(PutMergeCmd(), Math.min(generalSlotCount, aBufferCount))
      fifo.io.push << push
      val buffered = fifo.io.pop.combStage()
      val cmd = buffered.swapPayload(CMD())
      cmd.fromUpA := True
      cmd.toDownA := False
      cmd.toUpD := Directory.ToUpDOpcode.ACCESS_ACK
      cmd.gsId := buffered.gsId
      cmd.partialUpA := False
      cmd.address.assignDontCare()
      cmd.address(setsRange) := buffered.setId
      cmd.address(wordRange) := buffered.wordOffset
      cmd.size := buffered.size
      cmd.wayId := buffered.wayId
      cmd.bufferAId := buffered.bufferAId
      cmd.fromUpC := False
      cmd.toT := True
      cmd.source := buffered.source
      cmd.evict := False
    }

    val inserter = new Area {
      import inserterStage._


      val ctrlBuffered = ctrl.process.toWriteBackend
      val arbiter = StreamArbiterFactory().lowerFirst.transactionLock.buildOn(ctrlBuffered, putMerges.cmd, readBackend.process.toWriteBackend.halfPipe(), prober.schedule.toWriteBackend)
      val cmd = arbiter.io.output.pipelined(m2s = true, s2m = true)

      val counter = Reg(io.up.p.beat()) init (0)
      val upABeatsMinusOne = sizeToBeatMinusOne(io.up.p, cmd.size)
      val beatMax = CMD.fromUpC.mux(U(ubp.beatMax-1), upABeatsMinusOne)
      val LAST = insert(counter === beatMax)
      val addressWord = cmd.address(wordRange)
      val IN_UP_A = insert(!CMD.fromUpC || counter >= addressWord && counter <= addressWord + upABeatsMinusOne)

      cmd.ready := isReady && LAST
      valid := cmd.valid
      inserterStage(CMD) := cmd.payload
      val addressBase = cmd.address(refillRange) @@ cmd.address(refillRange.low-1 downto 0).andMask(!CMD.fromUpC)
      CMD.address.removeAssignments() := addressBase | (counter << log2Up(p.dataBytes)).resized

      when(isFireing) {
        counter := counter + 1
        when(LAST) {
          counter := 0
        }
      }
    }


    val fetch = new Area{
      import fetchStage._

      bufferA.read.cmd.valid := fetchStage.isFireing
      bufferA.read.cmd.payload := fetchStage(CMD).bufferAId @@ fetchStage(CMD).address(wordRange)
      when(isFireing && inserter.LAST && CMD.fromUpA) {
        bufferA.clear(fetchStage(CMD).bufferAId) := True
      }

      victimBuffer.read.cmd.valid := fetchStage.isFireing
      victimBuffer.read.cmd.payload := fetchStage(CMD).gsId @@ fetchStage(CMD).address(wordRange)
      val vh = readBackend.process
      val victimHazard = CMD.evict && (gs.slots.map(_.pending.victimRead).read(CMD.gsId) || vh.victimBusy && vh.victimGsId === CMD.gsId && vh.victimCounter === fetchStage(CMD).address(wordRange))
      haltWhen(victimHazard)
    }

    val BUFFER_A = readStage.insert(bufferA.read.rsp)
    val VICTIM = readStage.insert(victimBuffer.read.rsp)

    val process = new Area {
      import processStage._

      val aSplit = BUFFER_A.data.subdivideIn(8 bits)
      val cSplit = upCSplit.dataPop.payload.subdivideIn(8 bits)
      val selA = inserter.IN_UP_A && CMD.fromUpA
      val acMerge = (0 until dataBytes).map(i => (selA && BUFFER_A.mask(i)).mux(aSplit(i), cSplit(i)))
      val UP_DATA = insert((CMD.evict && !CMD.fromUpC).mux[Bits](VICTIM, acMerge.asBits))
      val UP_MASK = insert(BUFFER_A.mask.orMask(CMD.fromUpC || CMD.evict))
      val hazardUpC = CMD.fromUpC && !upCSplit.dataPop.valid
      upCSplit.dataPop.ready := CMD.fromUpC && isFireing

      val toCacheFork = forkStream(!CMD.toDownA)
      cache.data.upWrite.arbitrationFrom(toCacheFork.haltWhen(hazardUpC))
      cache.data.upWrite.address := CMD.wayId @@ CMD.address(setsRange.high downto wordRange.low)
      cache.data.upWrite.data := UP_DATA
      cache.data.upWrite.mask := UP_MASK

      val toDownAFork = forkStream(CMD.toDownA)
      val toDownA = toDownAFork.haltWhen(hazardUpC).swapPayload(io.down.a.payloadType)
      toDownA.opcode := (CMD.fromUpA && CMD.partialUpA).mux(Opcode.A.PUT_PARTIAL_DATA, Opcode.A.PUT_FULL_DATA)
      toDownA.param := 0
      toDownA.source := U(CMD.evict) @@ CMD.gsId
      toDownA.address := CMD.address
      toDownA.size := CMD.fromUpC.mux(U(log2Up(blockSize)), CMD.size)
      toDownA.data := UP_DATA
      toDownA.mask := UP_MASK
      toDownA.corrupt := False
      toDownA.debugId := DebugId.withPostfix(toDownA.source)

      import Directory.ToUpDOpcode._

      val needForkToUpD = CMD.toUpD.muxDc[Bool](
        NONE            -> False,
        ACCESS_ACK      -> inserter.LAST,
        RELEASE_ACK     -> inserter.LAST,
        GRANT           -> inserter.LAST,
        ACCESS_ACK_DATA -> inserter.IN_UP_A,
        GRANT_DATA      -> True
      )
      val toUpDFork = forkStream(needForkToUpD)
      val toUpD = toUpDFork.swapPayload(io.up.d.payloadType)
      toUpD.opcode  := CMD.toUpD.muxDc(
        ACCESS_ACK      -> Opcode.D.ACCESS_ACK(),
        ACCESS_ACK_DATA -> Opcode.D.ACCESS_ACK_DATA(),
        GRANT           -> Opcode.D.GRANT(),
        GRANT_DATA      -> Opcode.D.GRANT_DATA(),
        RELEASE_ACK     -> Opcode.D.RELEASE_ACK()
      )
      toUpD.param   := CMD.toT.mux[Bits](Param.Cap.toT, Param.Cap.toB).resized
      toUpD.source  := CMD.source
      toUpD.sink    := CMD.gsId
      toUpD.size    := CMD.size
      toUpD.denied  := False
      toUpD.data    := upCSplit.dataPop.payload //as it never come from a upA put
      toUpD.corrupt := False

      when(isFireing && inserter.LAST) {
        when(List(ACCESS_ACK, ACCESS_ACK_DATA, RELEASE_ACK).map(_()).sContains(CMD.toUpD)) {
          gs.slots.onSel(CMD.gsId) { s =>
            s.pending.primary := False
            s.pending.upC := False
          }
        }
      }

      val askOrdering = toUpD.isLast() && List(ACCESS_ACK, ACCESS_ACK_DATA, GRANT, GRANT_DATA).map(_()).sContains(CMD.toUpD)
      val toOrdering = forkFlow(askOrdering).swapPayload(io.ordering.writeBackend.payloadType)
      toOrdering.debugId := CMD.debugId
      toOrdering.bytes := (U(1) << CMD.size).resized
      toOrdering >> io.ordering.writeBackend
    }
  }



  val toDownA = new Area{
    val arbiter = StreamArbiterFactory().lowerFirst.lambdaLock[ChannelA](_.isLast()).build(io.down.a.payloadType, 2)
    arbiter.io.inputs(0) << readDown.toDownA
    arbiter.io.inputs(1) << writeBackend.process.toDownA
    io.down.a << arbiter.io.output
  }

  val fromDownD = new Pipeline{
    val stages = newChained(2, Connection.M2S())
    val inserterStage = stages(0)
    val fetchStage = stages(0)
    val readStage = stages(1)
    val processStage = stages(1)

    val CTX = Stageable(new CtxDownD())
    val inserter = new Area{
      import inserterStage._

      driveFrom(io.down.d)
      val CMD = insert(io.down.d.payload)
      val LAST = insert(io.down.d.isLast())
      val BEAT = insert(io.down.d.beatCounter())
    }

    import inserter._

    val readPort = gs.ctxDownD.ram.readSyncPort()
    readPort.cmd.valid := fetchStage.isFireing
    readPort.cmd.payload := fetchStage(CMD).source.resized
    readStage(CTX) := readPort.rsp

    val process = new Area{
      import processStage._

      val isVictim = CMD.source.msb
      val withData = CMD.opcode === Opcode.D.ACCESS_ACK_DATA


      //TODO handle refill while partial get to upD
      val toUpDHead = !withData || !CTX.toCache || (BEAT >= CTX.wordOffset && BEAT <= CTX.wordOffset + sizeToBeatMinusOne(io.down.p, CTX.size))
      val toUpD = forkStream(!isVictim && CTX.toUpD && toUpDHead).swapPayload(io.up.d.payloadType)

      toUpD.opcode  := CTX.acquire.mux(
        withData.mux(Opcode.D.GRANT_DATA, Opcode.D.GRANT),
        withData.mux(Opcode.D.ACCESS_ACK_DATA, Opcode.D.ACCESS_ACK)
      )
      toUpD.param   := CTX.toT.mux[Bits](Param.Cap.toT, Param.Cap.toB).resized
      toUpD.source  := CTX.sourceId
      toUpD.sink    := CMD.source.resized
      toUpD.size    := CTX.size
      toUpD.denied  := CMD.denied
      toUpD.data    := CMD.data
      toUpD.corrupt := CMD.corrupt

      val toCache = forkStream(!isVictim && CTX.toCache).swapPayload(cache.data.downWrite.payloadType)
      toCache.address := CTX.wayId @@ CTX.setId @@ BEAT
      toCache.data := CMD.data
      toCache.mask.setAll()

      val victimHazard = gs.slots.map(_.pending.victimRead).read(CMD.source.resized) || cache.data.readIntend && cache.data.read.payload === toCache.address
      toCache.haltWhen(victimHazard) >> cache.data.downWrite

      def putMerges = writeBackend.putMerges.push
      putMerges.valid      := False
      putMerges.gsId       := CMD.source.resized
      putMerges.setId      := CTX.setId
      putMerges.wayId      := CTX.wayId
      putMerges.wordOffset := CTX.wordOffset
      putMerges.bufferAId  := CTX.bufferAId
      putMerges.size       := CTX.size
      putMerges.source     := CTX.sourceId

      assert(!writeBackend.putMerges.push.isStall)

      when(isFireing && LAST) {
        when(isVictim) {
          gs.slots.onSel(CMD.source.resized)(_.pending.victim := False)
        } otherwise {
          when(CTX.mergeBufferA) {
            writeBackend.putMerges.push.valid := True
          }.elsewhen(!CTX.acquire) {
            gs.slots.onSel(CMD.source.resized)(_.pending.primary := False)
          }
        }
      }
    }
  }

  val toUpD = new Area{
    val arbiter = StreamArbiterFactory().lowerFirst.lambdaLock[ChannelD](_.isLast()).build(io.up.d.payloadType, 4)
    arbiter.io.inputs(0) << fromDownD.process.toUpD
    arbiter.io.inputs(1) << ctrl.process.toUpD.m2sPipe()
    arbiter.io.inputs(2) << readBackend.process.toUpD
    arbiter.io.inputs(3) << writeBackend.process.toUpD

    io.up.d << arbiter.io.output
  }

  val fromUpE = new Area{
    io.up.e.ready := True
    when(io.up.e.fire){
      gs.slots.onSel(io.up.e.sink)(_.pending.primary := False)
    }
  }

  ctrl.build()
  readBackend.build()
  writeBackend.build()
  fromDownD.build()

  when(!initializer.done) {
    cache.tags.write.valid := True
    cache.tags.write.address := initializer.counter.resized
    cache.tags.write.mask.setAll()
    cache.tags.write.data.clearAll()

    cache.plru.write.valid := True
    cache.plru.write.address := initializer.counter.resized
    cache.plru.write.data.clearAll()
  }
}


object DirectoryGen extends App{
  def basicConfig(generalSlotCount : Int = 8,
                  probeCount: Int = 4,
                  downPendingMax: Int = 16,
                  masterPerChannel: Int = 4,
                  dataWidth: Int = 64,
                  addressWidth: Int = 32,
                  lockSets: Int = 64*1024/64,
                  cacheBytes : Int = 64*1024,
                  cacheWays : Int = 8) = {
    val blockSize = 64
    DirectoryParam(
      unp = NodeParameters(
        m = M2sParameters(
          addressWidth = addressWidth,
          dataWidth = dataWidth,
          masters = List.tabulate(masterPerChannel)(mId =>
            M2sAgent(
              name = null,
              mapping = List.fill(1)(M2sSource(
                emits = M2sTransfers(
                  get = SizeRange(64),
                  putFull = SizeRange(64),
                  putPartial = SizeRange(64),
                  acquireT = SizeRange(64),
                  acquireB = SizeRange(64)
                ),
                id = SizeMapping(mId * 4, 4)
              ))
            )
          )
        ),
        s = S2mParameters(List(
          S2mAgent(
            name = null,
            emits = S2mTransfers(
              probe = SizeRange(64)
            ),
            sinkId = SizeMapping(0, generalSlotCount)
          )
        ))
      ),

      cacheWays = cacheWays,
      cacheBytes = cacheBytes,
      aBufferCount = 4,
      downPendingMax = downPendingMax,
      probeCount = probeCount,
      blockSize = blockSize,
      probeRegion = _ => True,
      generalSlotCount = generalSlotCount,
      allocateOnMiss = (_,_,_,_) => True
    )
  }

  SpinalVerilog(new Directory(basicConfig()))


  import spinal.lib.eda.bench._

  val rtls = ArrayBuffer[Rtl]()
  for (probeCount <- List(2)) { //Rtl.ffIo
    rtls += Rtl(SpinalVerilog((new Directory(basicConfig(dataWidth = 16, addressWidth = 32, cacheWays = 4)).setDefinitionName(s"Hub$probeCount"))))
  }
  val targets = XilinxStdTargets().take(2)

  Bench(rtls, targets)
}