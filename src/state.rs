//! Minimal turn state machine (ADR-004: tolerant reader, strict writer, poison-on-desync).

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    Idle,
    ThreadStarted,
    TurnStarted,
    Completed,
    Failed,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("invalid turn transition from {from:?} to {to:?}")]
    InvalidTransition { from: TurnState, to: TurnState },
}

#[derive(Debug)]
pub struct SessionState {
    turn: TurnState,
    poisoned: bool,
}

impl SessionState {
    pub fn new() -> Self {
        Self {
            turn: TurnState::Idle,
            poisoned: false,
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn poison(&mut self) {
        self.poisoned = true;
    }

    pub fn on_thread_started(&mut self) -> Result<(), StateError> {
        self.transition(TurnState::ThreadStarted)
    }

    pub fn on_turn_started(&mut self) -> Result<(), StateError> {
        self.transition(TurnState::TurnStarted)
    }

    pub fn on_turn_completed(&mut self) -> Result<(), StateError> {
        self.transition(TurnState::Completed)
    }

    pub fn on_turn_failed(&mut self) -> Result<(), StateError> {
        self.transition(TurnState::Failed)
    }

    fn transition(&mut self, to: TurnState) -> Result<(), StateError> {
        let allowed = matches!(
            (self.turn, to),
            (TurnState::Idle, TurnState::ThreadStarted)
                | (TurnState::ThreadStarted, TurnState::TurnStarted)
                | (TurnState::TurnStarted, TurnState::Completed)
                | (TurnState::TurnStarted, TurnState::Failed)
        );
        if !allowed {
            self.poisoned = true;
            return Err(StateError::InvalidTransition {
                from: self.turn,
                to,
            });
        }
        self.turn = to;
        Ok(())
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
    }

    #[test]
    fn impossible_transition_poisons_state() {
        let mut state = SessionState::new();
        assert!(state.on_turn_completed().is_err());
        assert!(state.is_poisoned());
    }
}
