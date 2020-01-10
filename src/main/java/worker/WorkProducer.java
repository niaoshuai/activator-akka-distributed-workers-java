package worker;

import akka.actor.ActorRef;
import akka.actor.Scheduler;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.forkjoin.ThreadLocalRandom;
import worker.Master.Work;

import java.util.UUID;

import static worker.Frontend.NotOk;
import static worker.Frontend.Ok;

/**
 * @author xx
 */
public class WorkProducer extends UntypedActor {

  private final ActorRef frontend;

  public WorkProducer(ActorRef frontend) {
    this.frontend = frontend;
  }

  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private Scheduler scheduler = getContext().system().scheduler();
  private ExecutionContext ec = getContext().system().dispatcher();
  private ThreadLocalRandom rnd = ThreadLocalRandom.current();

  private String nextWorkId() {
    return UUID.randomUUID().toString();
  }

  int n = 0;

  private static final Object TICK = new Object() {
    @Override
    public String toString() {
      return "Tick";
    }
  };

  @Override
  public void preStart() {
    scheduler.scheduleOnce(Duration.create(5, "seconds"), getSelf(), TICK, ec, getSelf());
  }

  /**
   * override postRestart so we don't call preStart and schedule a new Tick
   */
  @Override
  public void postRestart(Throwable reason) {
  }

  @Override
  public void onReceive(Object message) {
    if (message == TICK) {
      n += 1;
      log.info("Produced work: {}", n);
      Work work = new Work(nextWorkId(), n);
      frontend.tell(work, getSelf());
      getContext().become(waitAccepted(work), false);
    } else {
      unhandled(message);
    }
  }

  private Procedure<Object> waitAccepted(final Work work) {
    return new Procedure<Object>() {
      @Override
      public void apply(Object message) {
        if (message instanceof Ok) {
          getContext().unbecome();
          scheduler.scheduleOnce(Duration.create(rnd.nextInt(3, 10), "seconds"), getSelf(), TICK, ec, getSelf());
        } else if (message instanceof NotOk) {
          log.info("Work not accepted, retry after a while");
          scheduler.scheduleOnce(Duration.create(3, "seconds"), frontend, work, ec, getSelf());
        } else {
          unhandled(message);
        }
      }
    };
  }

}
