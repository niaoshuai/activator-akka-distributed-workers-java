package worker;

import worker.Master.Work;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author xx
 */
public final class WorkState {

  private final Map<String, Work> workInProgress;
  private final Set<String> acceptedWorkIds;
  private final Set<String> doneWorkIds;
  private final ConcurrentLinkedDeque<Work> pendingWork;


  public WorkState updated(WorkDomainEvent event) {
    WorkState newState = null;
    if (event instanceof WorkAccepted) {
      return new WorkState(this,(WorkAccepted) event);
    } else if (event instanceof WorkStarted) {
      return new WorkState(this,(WorkStarted) event);
    } else if (event instanceof WorkCompleted) {
      return new WorkState(this,(WorkCompleted) event);
    } else if (event instanceof WorkerFailed) {
      return new WorkState(this,(WorkerFailed) event);
    } else if (event instanceof WorkerTimedOut) {
      return new WorkState(this,(WorkerTimedOut) event);
    }
    return newState;
  }


  public WorkState() {
    workInProgress = new HashMap<String, Work>();
    acceptedWorkIds = new HashSet<String>();
    doneWorkIds = new HashSet<String>();
    pendingWork = new ConcurrentLinkedDeque<Work>();
  }

  private WorkState(WorkState workState, WorkAccepted workAccepted) {
    ConcurrentLinkedDeque<Work> tmpPendingWork = new ConcurrentLinkedDeque<Work>(workState.pendingWork);
    Set<String> tmpAcceptedWorkIds = new HashSet<String>(workState.acceptedWorkIds);
    tmpPendingWork.addLast(workAccepted.work);
    tmpAcceptedWorkIds.add(workAccepted.work.workId);
    workInProgress = new HashMap<String, Work>(workState.workInProgress);
    acceptedWorkIds = tmpAcceptedWorkIds;
    doneWorkIds = new HashSet<String>(workState.doneWorkIds);
    pendingWork = tmpPendingWork;


  }

  public WorkState(WorkState workState, WorkStarted workStarted) {
    ConcurrentLinkedDeque<Work> tmpPendingWork = new ConcurrentLinkedDeque<Work>(workState.pendingWork);
    Map<String, Work> tmpWorkInProgress = new HashMap<String, Work>(workState.workInProgress);

    Work work = tmpPendingWork.removeFirst();
    if (!work.workId.equals(workStarted.workId)) {
      throw new IllegalArgumentException("WorkStarted expected workId " + work.workId + "==" + workStarted.workId);
    }
    tmpWorkInProgress.put(work.workId, work);

    workInProgress = tmpWorkInProgress;
    acceptedWorkIds = new HashSet<String>(workState.acceptedWorkIds);
    ;
    doneWorkIds = new HashSet<String>(workState.doneWorkIds);
    pendingWork = tmpPendingWork;
  }

  public WorkState(WorkState workState, WorkCompleted workCompleted) {
    Map<String, Work> tmpWorkInProgress = new HashMap<String, Work>(workState.workInProgress);
    Set<String> tmpDoneWorkIds = new HashSet<String>(workState.doneWorkIds);
    tmpWorkInProgress.remove(workCompleted.workId);
    tmpDoneWorkIds.add(workCompleted.workId);
    workInProgress = tmpWorkInProgress;
    acceptedWorkIds = new HashSet<String>(workState.acceptedWorkIds);
    ;
    doneWorkIds = tmpDoneWorkIds;
    pendingWork = new ConcurrentLinkedDeque<Work>(workState.pendingWork);
    ;
  }


  public WorkState(WorkState workState, WorkerFailed workerFailed) {
    Map<String, Work> tmpWorkInProgress = new HashMap<String, Work>(workState.workInProgress);
    ConcurrentLinkedDeque<Work> tmpPendingWork = new ConcurrentLinkedDeque<Work>(workState.pendingWork);
    tmpPendingWork.addLast(workState.workInProgress.get(workerFailed.workId));
    tmpWorkInProgress.remove(workerFailed.workId);
    workInProgress = tmpWorkInProgress;
    acceptedWorkIds = new HashSet<String>(workState.acceptedWorkIds);
    ;
    doneWorkIds = new HashSet<String>(workState.doneWorkIds);
    pendingWork = tmpPendingWork;
  }


  public WorkState(WorkState workState, WorkerTimedOut workerTimedOut) {
    Map<String, Work> tmpWorkInProgress = new HashMap<String, Work>(workState.workInProgress);
    ConcurrentLinkedDeque<Work> tmpPendingWork = new ConcurrentLinkedDeque<Work>(workState.pendingWork);
    tmpPendingWork.addLast(workState.workInProgress.get(workerTimedOut.workId));
    tmpWorkInProgress.remove(workerTimedOut.workId);
    workInProgress = tmpWorkInProgress;
    acceptedWorkIds = new HashSet<String>(workState.acceptedWorkIds);
    ;
    doneWorkIds = new HashSet<String>(workState.doneWorkIds);
    pendingWork = tmpPendingWork;
  }




  public void test() {
    for (Work work : pendingWork) {
      System.out.println(work.job);
    }
  }


  @Override
  public String toString() {
    return ""+acceptedWorkIds.size();
  }

  public Work nextWork() {
    return pendingWork.getFirst();
  }

  public boolean hasWork() {
    return !pendingWork.isEmpty();
  }

  public boolean isAccepted(String workId) {
    return acceptedWorkIds.contains(workId);
  }

  public boolean isInProgress(String workId) {
    return workInProgress.containsKey(workId);
  }

  public boolean isDone(String workId) {
    return doneWorkIds.contains(workId);
  }

  public static final class WorkAccepted implements WorkDomainEvent, Serializable {
    final Work work;

    public WorkAccepted(Work work) {
      this.work = work;
    }

  }

  public static final class WorkStarted implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkStarted(String workId) {
      this.workId = workId;
    }

  }

  public static final class WorkCompleted implements WorkDomainEvent, Serializable {
    final String workId;
    final Object result;

    public WorkCompleted(String workId,Object result) {
      this.workId = workId;
      this.result = result;
    }

  }

  public static final class WorkerFailed implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkerFailed(String workId) {
      this.workId = workId;
    }

  }

  public static final class WorkerTimedOut implements WorkDomainEvent, Serializable {
    final String workId;

    public WorkerTimedOut(String workId) {
      this.workId = workId;
    }

  }

}
