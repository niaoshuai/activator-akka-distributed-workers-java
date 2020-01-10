package worker;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import akka.persistence.UntypedPersistentActor;
import scala.collection.JavaConversions;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import worker.WorkState.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static worker.BaseMasterWorkerProtocol.*;

/**
 * @author xx
 */
public class Master extends UntypedPersistentActor {

  public static String ResultsTopic = "results";

  public static Props props(FiniteDuration workTimeout) {
    return Props.create(Master.class, workTimeout);
  }

  private final FiniteDuration workTimeout;
  private final ActorRef mediator = DistributedPubSub.get(getContext().system()).mediator();
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  private final Cancellable cleanupTask;

  private HashMap<String, WorkerState> workers = new HashMap<String, WorkerState>();
  private WorkState workState = new WorkState();

  public static final Object CLEANUP_TICK = new Object() {
    @Override
    public String toString() {
      return "CLEANUP_TICK";
    }
  };

  @Override
  public void postStop() {
    cleanupTask.cancel();
  }

  public Master(FiniteDuration workTimeout) {
    this.workTimeout = workTimeout;
    ClusterClientReceptionist.get(getContext().system()).registerService(getSelf());
    this.cleanupTask = getContext().system().scheduler().schedule(workTimeout.div(2), workTimeout.div(2), getSelf(), CLEANUP_TICK, getContext().dispatcher(), getSelf());
  }

  private void notifyWorkers() {
    if (workState.hasWork()) {
      // could pick a few random instead of all
      for (WorkerState state : workers.values()) {
        if (state.status.isIdle()) {
          state.ref.tell(WorkIsReady.getInstance(), getSelf());
        }
      }
    }
  }

  @Override
  public void onReceiveCommand(Object cmd) throws Exception {
    if (cmd instanceof RegisterWorker) {
      doRegisterCmd((RegisterWorker) cmd);
    } else if (cmd instanceof WorkerRequestsWork) {
      doWorkerRequestCmd((WorkerRequestsWork) cmd);
    } else if (cmd instanceof WorkIsDone) {
      doWorkDoneCmd((WorkIsDone) cmd);
    } else if (cmd instanceof WorkFailed) {
      doWorkFaildCmd((WorkFailed) cmd);
    } else if (cmd instanceof Work) {
      doWorkCmd((Work) cmd);
    } else if (cmd == CLEANUP_TICK) {
      doCleanUpCmd();
    } else {
      unhandled(cmd);
    }
  }

  private void doCleanUpCmd() {
    Iterator<Map.Entry<String, WorkerState>> iterator = workers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, WorkerState> entry = iterator.next();
      String workerId = entry.getKey();
      WorkerState state = entry.getValue();
      if (state.status.isBusy()) {
        if (state.status.getDeadLine().isOverdue()) {
          log.info("Work timed out: {}", state.status.getWorkId());
          workers.remove(workerId);
          persist(new WorkerTimedOut(state.status.getWorkId()), new Procedure<WorkerTimedOut>() {
            @Override
            public void apply(WorkerTimedOut event) throws Exception {
              workState = workState.updated(event);
              notifyWorkers();
            }
          });
        }
      }
    }
  }

  private void doWorkCmd(Work cmd) {
    final String workId = cmd.workId;
    // idempotent
    if (workState.isAccepted(workId)) {
      getSender().tell(new Ack(workId), getSelf());
    } else {
      log.info("Accepted work: {}", workId);
      persist(new WorkAccepted(cmd), new Procedure<WorkAccepted>() {
        @Override
        public void apply(WorkAccepted event) throws Exception {
          // Ack back to original sender
          getSender().tell(new Ack(event.work.workId), getSelf());
          workState = workState.updated(event);
          notifyWorkers();
        }
      });
    }
  }

  private void doWorkFaildCmd(WorkFailed cmd) {
    final String workId = cmd.workId;
    final String workerId = cmd.workerId;
    if (workState.isInProgress(workId)) {
      log.info("Work {} failed by worker {}", workId, workerId);
      changeWorkerToIdle(workerId, workId);
      persist(new WorkerFailed(workId), new Procedure<WorkerFailed>() {
        @Override
        public void apply(WorkerFailed event) throws Exception {
          workState = workState.updated(event);
          notifyWorkers();
        }
      });
    }
  }

  public static final class Work implements Serializable {
    public final String workId;
    public final Object job;

    public Work(String workId, Object job) {
      this.workId = workId;
      this.job = job;
    }

    @Override
    public String toString() {
      return "Work{" + "workId='" + workId + '\'' + ", job=" + job + '}';
    }
  }

  public static final class WorkResult implements Serializable {
    public final String workId;
    public final Object result;

    public WorkResult(String workId, Object result) {
      this.workId = workId;
      this.result = result;
    }

    @Override
    public String toString() {
      return "WorkResult{" + "workId='" + workId + '\'' + ", result=" + result + '}';
    }
  }

  public static final class Ack implements Serializable {
    final String workId;

    public Ack(String workId) {
      this.workId = workId;
    }

    @Override
    public String toString() {
      return "Ack{" + "workId='" + workId + '\'' + '}';
    }
  }

  @Override
  public void onReceiveRecover(Object arg0) throws Exception {
    if (arg0 instanceof WorkDomainEvent) {
      workState = workState.updated((WorkDomainEvent) arg0);
      log.info("Replayed {}", arg0.getClass().getSimpleName());
    }
  }

  @Override
  public String persistenceId() {
    for (String role : JavaConversions.asJavaIterable((Cluster.get(getContext().system()).selfRoles()))) {
      if (role.startsWith("backend-")) {
        return role + "-master";
      }
    }
    return "master";

  }

  private void doWorkDoneCmd(WorkIsDone cmd) {
    final String workerId = cmd.workerId;
    final String workId = cmd.workId;
    if (workState.isDone(workId)) {
      getSender().tell(new Ack(workId), getSelf());
    } else if (!workState.isInProgress(workId)) {
      log.info("Work {} not in progress, reported as done by worker {}", workId, workerId);
    } else {
      log.info("Work {} is done by worker {}", workId, workerId);
      changeWorkerToIdle(workerId, workId);
      persist(new WorkCompleted(workId, cmd.result), new Procedure<WorkCompleted>() {
        @Override
        public void apply(WorkCompleted event) throws Exception {
          workState = workState.updated(event);
          mediator.tell(new DistributedPubSubMediator.Publish(ResultsTopic, new WorkResult(event.workId, event.result)), getSelf());
          getSender().tell(new Ack(event.workId), getSelf());
        }
      });
    }
  }

  private void doWorkerRequestCmd(WorkerRequestsWork cmd) {
    if (workState.hasWork()) {
      final String workerId = cmd.workerId;
      final WorkerState state = workers.get(workerId);
      if (state != null && state.status.isIdle()) {
        final Work work = workState.nextWork();
        persist(new WorkStarted(work.workId), new Procedure<WorkStarted>() {
          @Override
          public void apply(WorkStarted event) throws Exception {
            workState = workState.updated(event);
            log.info("Giving worker {} some work {}", workerId, event.workId);
            workers.put(workerId, state.copyWithStatus(new Busy(event.workId, workTimeout.fromNow())));
            getSender().tell(work, getSelf());

          }
        });
      }
    }
  }

  private void doRegisterCmd(RegisterWorker cmd) {
    String workerId = cmd.workerId;
    if (workers.containsKey(workerId)) {
      workers.put(workerId, workers.get(workerId).copyWithRef(getSender()));
    } else {
      log.info("Worker registered: {}", workerId);
      workers.put(workerId, new WorkerState(getSender(), Idle.INSTANCE));
      if (workState.hasWork()) {
        getSender().tell(WorkIsReady.getInstance(), getSelf());
      }
    }
  }

  private static abstract class BaseWorkerStatus {
    /**
     * 空闲状态
     *
     * @return
     */
    protected abstract boolean isIdle();

    /**
     * 繁忙状态
     *
     * @return
     */
    private boolean isBusy() {
      return !isIdle();
    }

    ;

    /**
     * 获取workId
     *
     * @return
     */
    protected abstract String getWorkId();

    /**
     * deadLine
     *
     * @return
     */
    protected abstract Deadline getDeadLine();
  }

  private static final class Idle extends BaseWorkerStatus {
    private static final Idle INSTANCE = new Idle();

    public static Idle getInstance() {
      return INSTANCE;
    }

    @Override
    protected boolean isIdle() {
      return true;
    }

    @Override
    protected String getWorkId() {
      throw new IllegalAccessError();
    }

    @Override
    protected Deadline getDeadLine() {
      throw new IllegalAccessError();
    }

    @Override
    public String toString() {
      return "Idle";
    }
  }

  private static final class Busy extends BaseWorkerStatus {
    private final String workId;
    private final Deadline deadline;

    private Busy(String workId, Deadline deadline) {
      this.workId = workId;
      this.deadline = deadline;
    }

    @Override
    protected boolean isIdle() {
      return false;
    }

    @Override
    protected String getWorkId() {
      return workId;
    }

    @Override
    protected Deadline getDeadLine() {
      return deadline;
    }

    @Override
    public String toString() {
      return "Busy{" + "work=" + workId + ", deadline=" + deadline + '}';
    }
  }

  private static final class WorkerState {
    public final ActorRef ref;
    public final BaseWorkerStatus status;

    private WorkerState(ActorRef ref, BaseWorkerStatus status) {
      this.ref = ref;
      this.status = status;
    }

    private WorkerState copyWithRef(ActorRef ref) {
      return new WorkerState(ref, this.status);
    }

    private WorkerState copyWithStatus(BaseWorkerStatus status) {
      return new WorkerState(this.ref, status);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || !getClass().equals(o.getClass())) {
        return false;
      }

      WorkerState that = (WorkerState) o;

      if (!ref.equals(that.ref)) {
        return false;
      }
      if (!status.equals(that.status)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = ref.hashCode();
      result = 31 * result + status.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "WorkerState{" + "ref=" + ref + ", status=" + status + '}';
    }
  }

  private void changeWorkerToIdle(String workerId, String workId) {
    if (workers.get(workerId).status.isBusy()) {
      workers.put(workerId, workers.get(workerId).copyWithStatus(new Idle()));
    }
  }
}
