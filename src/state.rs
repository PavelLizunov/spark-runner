//! Minimal worker/turn state machine (ADR-004: tolerant reader, strict writer,
//! poison-on-desync) plus typed internal events for lifecycle and approvals.

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Idle,
    ThreadActive,
    TurnActive,
    ShuttingDown,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Idle,
    ThreadStarted,
    TurnStarted,
    AwaitingApproval,
    Interrupted,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    Allow,
    Deny,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalSource {
    Owner,
    Model,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalEventKind {
    WorkerTransition {
        from: WorkerState,
        to: WorkerState,
    },
    TurnTransition {
        from: TurnState,
        to: TurnState,
    },
    ApprovalRequested {
        request_key: String,
        method: String,
    },
    ApprovalDecided {
        request_key: String,
        method: String,
        decision: ApprovalDecision,
    },
    InterruptRequested {
        thread_id: String,
        turn_id: String,
    },
    Poisoned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalEvent {
    pub seq: u64,
    pub kind: InternalEventKind,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("invalid worker transition from {from:?} to {to:?}")]
    InvalidWorkerTransition { from: WorkerState, to: WorkerState },
    #[error("invalid turn transition from {from:?} to {to:?}")]
    InvalidTurnTransition { from: TurnState, to: TurnState },
    #[error("model output cannot approve request {request_key}")]
    ModelSelfApproval { request_key: String },
}

#[derive(Debug)]
pub struct SessionState {
    worker: WorkerState,
    turn: TurnState,
    poisoned: bool,
    next_event_seq: u64,
    events: Vec<InternalEvent>,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            worker: WorkerState::Idle,
            turn: TurnState::Idle,
            poisoned: false,
            next_event_seq: 1,
            events: Vec::new(),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn events(&self) -> &[InternalEvent] {
        &self.events
    }

    pub fn poison(&mut self) {
        if !self.poisoned {
            self.poisoned = true;
            self.emit(InternalEventKind::Poisoned);
        }
    }

    pub fn on_thread_started(&mut self) -> Result<(), StateError> {
        self.transition_worker(WorkerState::ThreadActive)?;
        self.transition_turn(TurnState::ThreadStarted)
    }

    pub fn on_turn_started(&mut self) -> Result<(), StateError> {
        self.transition_worker(WorkerState::TurnActive)?;
        self.transition_turn(TurnState::TurnStarted)
    }

    pub fn on_approval_requested(
        &mut self,
        request_key: String,
        method: String,
    ) -> Result<(), StateError> {
        self.transition_turn(TurnState::AwaitingApproval)?;
        self.emit(InternalEventKind::ApprovalRequested {
            request_key,
            method,
        });
        Ok(())
    }

    pub fn on_approval_decided(
        &mut self,
        request_key: String,
        method: String,
        decision: ApprovalDecision,
        source: ApprovalSource,
    ) -> Result<(), StateError> {
        if source == ApprovalSource::Model && decision == ApprovalDecision::Allow {
            self.poison();
            return Err(StateError::ModelSelfApproval { request_key });
        }
        self.emit(InternalEventKind::ApprovalDecided {
            request_key,
            method,
            decision,
        });
        match decision {
            ApprovalDecision::Allow => self.transition_turn(TurnState::TurnStarted),
            ApprovalDecision::Deny | ApprovalDecision::Timeout => {
                self.transition_turn(TurnState::Interrupted)
            }
        }
    }

    pub fn on_interrupt_requested(
        &mut self,
        thread_id: String,
        turn_id: String,
    ) -> Result<(), StateError> {
        self.emit(InternalEventKind::InterruptRequested { thread_id, turn_id });
        self.transition_turn(TurnState::Interrupted)
    }

    pub fn on_turn_completed(&mut self) -> Result<(), StateError> {
        self.transition_turn(TurnState::Completed)
    }

    pub fn on_turn_failed(&mut self) -> Result<(), StateError> {
        self.transition_turn(TurnState::Failed)
    }

    pub fn on_shutdown(&mut self) -> Result<(), StateError> {
        self.transition_worker(WorkerState::ShuttingDown)?;
        self.transition_worker(WorkerState::Stopped)
    }

    fn transition_worker(&mut self, to: WorkerState) -> Result<(), StateError> {
        let allowed = matches!(
            (self.worker, to),
            (WorkerState::Idle, WorkerState::ThreadActive)
                | (WorkerState::ThreadActive, WorkerState::TurnActive)
                | (WorkerState::ThreadActive, WorkerState::ShuttingDown)
                | (WorkerState::TurnActive, WorkerState::ShuttingDown)
                | (WorkerState::ShuttingDown, WorkerState::Stopped)
        );
        if !allowed {
            self.poison();
            return Err(StateError::InvalidWorkerTransition {
                from: self.worker,
                to,
            });
        }
        let from = self.worker;
        self.worker = to;
        self.emit(InternalEventKind::WorkerTransition { from, to });
        Ok(())
    }

    fn transition_turn(&mut self, to: TurnState) -> Result<(), StateError> {
        let allowed = matches!(
            (self.turn, to),
            (TurnState::Idle, TurnState::ThreadStarted)
                | (TurnState::ThreadStarted, TurnState::TurnStarted)
                | (TurnState::TurnStarted, TurnState::AwaitingApproval)
                | (TurnState::AwaitingApproval, TurnState::TurnStarted)
                | (TurnState::AwaitingApproval, TurnState::Interrupted)
                | (TurnState::TurnStarted, TurnState::Interrupted)
                | (TurnState::TurnStarted, TurnState::Completed)
                | (TurnState::TurnStarted, TurnState::Failed)
                | (TurnState::Interrupted, TurnState::Failed)
        );
        if !allowed {
            self.poison();
            return Err(StateError::InvalidTurnTransition {
                from: self.turn,
                to,
            });
        }
        let from = self.turn;
        self.turn = to;
        self.emit(InternalEventKind::TurnTransition { from, to });
        Ok(())
    }

    fn emit(&mut self, kind: InternalEventKind) {
        let seq = self.next_event_seq;
        self.next_event_seq += 1;
        self.events.push(InternalEvent { seq, kind });
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_transitions() {
        let mut state = SessionState::new();
        state.on_thread_started().unwrap();
        state.on_turn_started().unwrap();
        state.on_turn_completed().unwrap();
        assert_eq!(state.turn, TurnState::Completed);
        assert!(!state.is_poisoned());
        assert_eq!(state.events()[0].seq, 1);
        assert_eq!(state.events()[1].seq, 2);
    }

    #[test]
    fn impossible_transition_poisons_state() {
        let mut state = SessionState::new();
        assert!(state.on_turn_completed().is_err());
        assert!(state.is_poisoned());
    }

    #[test]
    fn terminal_reverse_transition_is_rejected() {
        let mut state = SessionState::new();
        state.on_thread_started().unwrap();
        state.on_turn_started().unwrap();
        state.on_turn_completed().unwrap();
        assert!(state.on_turn_started().is_err());
        assert!(state.is_poisoned());
    }

    #[test]
    fn model_cannot_self_approve() {
        let mut state = SessionState::new();
        state.on_thread_started().unwrap();
        state.on_turn_started().unwrap();
        state
            .on_approval_requested("approval-1".to_string(), "method".to_string())
            .unwrap();
        assert!(state
            .on_approval_decided(
                "approval-1".to_string(),
                "method".to_string(),
                ApprovalDecision::Allow,
                ApprovalSource::Model,
            )
            .is_err());
        assert!(state.is_poisoned());
    }
}
