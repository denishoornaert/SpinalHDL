package spinal.lib.bus.tilelink

import spinal.core._
import spinal.lib._
import scala.collection.Seq

object Arbiter{
  def downMastersFrom(ups : Seq[M2sParameters]) : M2sParameters = {
    NodeParameters.mergeMasters(ups)
  }
  def downNodeFrom(ups : Seq[NodeParameters]) : NodeParameters = {
    NodeParameters.mergeMasters(ups)
  }
  def upSlaveFrom(down : S2mParameters, up : S2mSupport) : S2mParameters = {
    up.transfers.withAny match {
      case true => down.copy(
        slaves = down.slaves.map(e =>
          e.copy(
            emits = e.emits.intersect(up.transfers)
          )
        )
      )
      case false => S2mParameters.none()
    }
  }
}

case class Arbiter(upsNodes : Seq[NodeParameters], downNode : NodeParameters) extends Component{
  val obp = downNode //Arbiter.downNodeFrom(upsNodes)
  val io = new Bundle{
    val ups = Vec(upsNodes.map(e => slave(Bus(e))))
    val down = master(Bus(obp))
  }

  val sourceOffsetWidth = log2Up(upsNodes.size)
  val perNodeSourceWidth = upsNodes.map(_.m.sourceWidth).max
  val ups = io.ups.zipWithIndex.map{case (bus, id) => bus.withSourceOffset(id << perNodeSourceWidth, obp.m.sourceWidth)}

  val a = new Area{
    // val arbiter = StreamArbiterFactory().roundRobin.lambdaLock[ChannelA](_.isLast()).build(ChannelA(obp.toBusParameter()), upsNodes.size)
    def rtcciPrio(core: StreamArbiter[ChannelA]) = new Area {
      for(bitId  <- core.maskLocked.range){
        core.maskLocked(bitId) init(Bool(bitId == core.maskLocked.length-1))
      }

      // 1. Get the priorities of the ups
      val prios = Vec(core.io.inputs.map(_.payload.prio))

      // 2. Create a list with masks of which ups have a specific priority
      // TODO: implicitly assumes all ups have same prioWidth
      val prio_masks = Vec(Bits(core.io.inputs.size bits), core.io.inputs(0).prio.valueRange.size)
      for (prioId <- core.io.inputs(0).prio.valueRange) {
        prio_masks(prioId) :=  core.io.inputs.map(x => x.valid & (x.payload.prio === prioId)).asBits
      }
      
      // 3. Create a mask for the which elements in the list have at least one bit set
      val prio_masks_mask = prio_masks.map(_.orR)

      // 4. Choose the first mask in the list (e.g. the mask of the ups with highest priority)
      val prio_mask = prio_masks.oneHotAccess(OHMasking.first(prio_masks_mask.asBits))

      // 5. Put the obtained mask into the round robin logic below:
      core.maskProposal := OHMasking.roundRobin(
        prio_mask.asBools,
        Vec(core.maskLocked.last +: core.maskLocked.take(core.maskLocked.length-1))
      )
      

      /*
      core.maskProposal := OHMasking.roundRobin(
        Vec(core.io.inputs.map(_.valid)),
        Vec(core.maskLocked.last +: core.maskLocked.take(core.maskLocked.length-1))
      )
      */
    }
    val lockLogic = StreamArbiterFactory().lambdaLock[ChannelA](_.isLast()).lockLogic
    val arbiter = new StreamArbiter(ChannelA(obp.toBusParameter()), upsNodes.size)(rtcciPrio, lockLogic)

//    (arbiter.io.inputs, ups).zipped.foreach(_ connectFromRelaxed _.a)
    (arbiter.io.inputs, ups).zipped.foreach{(arb, up) =>
      arb.arbitrationFrom(up.a)
      arb.payload.weakAssignFrom(up.a.payload)
    }
    arbiter.io.output >> io.down.a
//    io.down.a.source(obp.m.sourceWidth-sourceOffsetWidth, sourceOffsetWidth bits) := arbiter.io.chosen
  }

  val b = obp.withBCE generate new Area{
    val sel = io.down.b.source.takeHigh(sourceOffsetWidth).asUInt
    io.down.b.ready := ups.map(e => if(e.p.withBCE) e.b.ready else False).read(sel)
    for((s, id) <- ups.zipWithIndex if s.p.withBCE) {
      val hit = sel === id
      s.b.valid := io.down.b.valid && hit
      s.b.payload := io.down.b.payload
    }
  }

  val c = obp.withBCE generate new Area{
    val arbiter = StreamArbiterFactory().roundRobin.lambdaLock[ChannelC](_.isLast()).build(ChannelC(obp.toBusParameter()), upsNodes.filter(_.withBCE).size)
    (arbiter.io.inputs, ups.filter(_.p.withBCE)).zipped.foreach(_ << _.c)
    arbiter.io.output >> io.down.c
//    io.down.c.source(obp.m.sourceWidth-sourceOffsetWidth, sourceOffsetWidth bits) := arbiter.io.chosen
  }

  val d = new Area{
    val sel = io.down.d.source.takeHigh(sourceOffsetWidth).asUInt
    io.down.d.ready := ups.map(_.d.ready).read(sel)
    for((s, id) <- ups.zipWithIndex){
      val hit = sel === id
      s.d.valid := io.down.d.valid && hit
      s.d.payload.weakAssignFrom(io.down.d.payload)
      if(!s.p.withBCE) s.d.sink.removeAssignments() := 0
    }
  }

  val e = obp.withBCE generate new Area{
    val arbiter = StreamArbiterFactory().roundRobin.transactionLock.build(ChannelE(obp.toBusParameter()), upsNodes.filter(_.withBCE).size)
    (arbiter.io.inputs, ups.filter(_.p.withBCE)).zipped.foreach(_ << _.e)
    arbiter.io.output >> io.down.e
  }
}
