use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
};

use crate::dataflow::Timestamp;

/// Trait that must be implemented by stream state structs.
pub trait State: 'static + Clone {}
impl<T: 'static + Clone> State for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessError(&'static str);

/// In what context is the operator accessed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AccessContext {
    /// In either `Operator::new` when the `TimeVersionedState` is created.
    /// Gives access to `TimeVersionedState::set_history_size` and
    /// `TimeVersionedState::set_initial_state`,
    Operator,
    /// A regular non-watermark callback.
    /// Gives access to `TimeVersionedState::append`.
    Callback,
    /// A watermark callback.
    /// Gives access to `TimeVersionedState::get_current_messages`,
    /// `TimeVersionedState::get_state`, `TimeVersionedState::get_current_state`,
    /// `TimeVersionedState::get_current_state_mut`, `TimeVersionedState::iter_states`.
    WatermarkCallback,
}

pub struct TimeVersionedState<S: State + Default, T: Clone> {
    current_time: Timestamp,
    // The number of past states to keep.
    history_size: usize,
    // Determines access control rules.
    access_context: AccessContext,
    message_history: BTreeMap<Timestamp, Vec<T>>,
    state_history: BTreeMap<Timestamp, S>,
}

impl<S: State + Default, T: Clone> TimeVersionedState<S, T> {
    pub fn new() -> Self {
        Self::new_with_history_size(0)
    }

    pub fn new_with_history_size(history_size: usize) -> Self {
        Self {
            current_time: Timestamp::bottom(),
            history_size,
            access_context: AccessContext::Operator,
            message_history: BTreeMap::new(),
            state_history: BTreeMap::new(),
        }
    }

    pub(crate) fn set_access_context(&mut self, access_context: AccessContext) {
        self.access_context = access_context;
    }

    /// Updates access rules and initializes state and message history for current time.
    pub(crate) fn set_current_time(&mut self, t: Timestamp) {
        self.current_time = t;
        self.message_history
            .entry(self.current_time.clone())
            .or_default();
        self.state_history
            .entry(self.current_time.clone())
            .or_default();
    }

    /// Garbage collects state and messages no longer needed after the last watermark callback
    /// operating over t completes.
    pub(crate) fn close_time(&mut self, t: &Timestamp) {
        // Release all messages until and including t.
        self.message_history = self.message_history.split_off(&t);
        self.message_history.remove(&t);

        // Release all states at least as old as history_size timestamps before t.
        // Clean this up if BTreeMap adds more detailed query methods in future Rust versions.
        let split_option = if self.history_size == 0 {
            let mut range = self
                .state_history
                .range((Excluded(t.clone()), Unbounded))
                .map(|x| x.0);
            range.next()
        } else {
            let mut range = self.state_history.range(..=t.clone()).map(|x| x.0);
            for _ in 0..(self.history_size - 1) {
                range.next_back();
            }
            range.next_back()
        };
        if let Some(split_t_ref) = split_option {
            // Avoid E0502: mutable borrow on state_history while split_t_ref is borrowed
            // immutably.
            let split_t = split_t_ref.clone();
            self.state_history = self.state_history.split_off(&split_t);
        }
    }

    pub fn history_size(&self) -> usize {
        self.history_size
    }

    /// Sets the number of past states available.
    /// Only accessible from Operator::new.
    pub fn set_history_size(&mut self, history_size: usize) -> Result<(), AccessError> {
        match self.access_context {
            AccessContext::Operator => {
                self.history_size = history_size;
                Ok(())
            }
            AccessContext::Callback => {
                Err(AccessError("Attempted to set_history_size from callback"))
            }
            AccessContext::WatermarkCallback => Err(AccessError(
                "Attempted to set_history_size from watermark callback",
            )),
        }
    }

    /// Sets the initial state stored for `Timestamp::bottom`.
    /// Only accessible from Operator::new.
    pub fn set_initial_state(&mut self, initial_state: S) -> Result<(), AccessError> {
        match self.access_context {
            AccessContext::Operator => {
                self.state_history
                    .insert(Timestamp::bottom(), initial_state);
                Ok(())
            }
            AccessContext::Callback => {
                Err(AccessError("Attempted to set_initial_state from callback"))
            }
            AccessContext::WatermarkCallback => Err(AccessError(
                "Attempted to set_initial_state from watermark callback",
            )),
        }
    }

    /// Appends a message to the message history.
    /// Only accessible from regular callbacks.
    pub fn append(&mut self, data: T) -> Result<(), AccessError> {
        match self.access_context {
            AccessContext::Operator => Err(AccessError("Attempted to append from Operator::new")),
            AccessContext::Callback => {
                self.message_history
                    .get_mut(&self.current_time)
                    .expect(&format!(
                        "ERDOS internal error: message history vector not initialized for {:?}.",
                        self.current_time
                    ))
                    .push(data);
                Ok(())
            }
            AccessContext::WatermarkCallback => {
                Err(AccessError("Attempted to append from a watermark callback"))
            }
        }
    }

    /// Gets the message history for the current time.
    /// Can only be called by watermark callbacks, in `Operator::new()`, or in `Operator::run()`.
    pub fn get_current_messages(&self) -> Result<&Vec<T>, AccessError> {
        match self.access_context {
            AccessContext::Operator => Err(AccessError(
                "Attempted to get_current_messages from Operator::new",
            )),
            AccessContext::WatermarkCallback => Ok(self
                .message_history
                .get(&self.current_time)
                .unwrap_or_else(|| {
                    panic!(
                        "ERDOS internal error: message history not initialized for {:?}",
                        self.current_time
                    )
                })),
            AccessContext::Callback => Err(AccessError(
                "Attempted to get_current_messages from a non-watermark callback",
            )),
        }
    }

    /// Gets an immutable reference to the state at the provied timestamp.
    pub fn get_state(&self, t: &Timestamp) -> Result<Option<&S>, AccessError> {
        match self.access_context {
            AccessContext::Operator => {
                Err(AccessError("Attempted to get_state from Operator::new"))
            }
            AccessContext::WatermarkCallback => {
                if t <= &self.current_time {
                    let mut iter = self
                        .state_history
                        .range(t.clone()..self.current_time.clone());
                    if let Some((oldest_allowed_t, _)) = iter.nth_back(self.history_size()) {
                        if oldest_allowed_t <= t {
                            Ok(self.state_history.get(t))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(self.state_history.get(t))
                    }
                } else {
                    Ok(None)
                }
            }
            AccessContext::Callback => Err(AccessError(
                "Attempted to get_state from a non-watermark callback",
            )),
        }
    }

    /// Gets an immutable reference to the state for the current timestamp.
    pub fn get_current_state(&self) -> Result<&S, AccessError> {
        match self.access_context {
            AccessContext::Operator => Err(AccessError(
                "Attempted to get_current_state from Operator::new",
            )),
            AccessContext::WatermarkCallback => {
                Ok(self.state_history.get(&self.current_time).expect(&format!(
                    "ERDOS interal error: state not initialized or {:?} (current timestamp).",
                    self.current_time
                )))
            }
            AccessContext::Callback => Err(AccessError(
                "Attempted to get_current_state from a non-watermark callback",
            )),
        }
    }

    /// Gets a mutable reference to the state for the current timestamp.
    pub fn get_current_state_mut(&mut self) -> Result<&mut S, AccessError> {
        match self.access_context {
            AccessContext::Operator => Err(AccessError(
                "Attempted to get_current_state_mut from Operator::new",
            )),
            AccessContext::WatermarkCallback => Ok(self
                .state_history
                .get_mut(&self.current_time)
                .expect(&format!(
                    "ERDOS interal error: state not initialized or {:?} (current timestamp).",
                    self.current_time
                ))),
            AccessContext::Callback => Err(AccessError(
                "Attempted to get_current_state_mut from a non-watermark callback",
            )),
        }
    }

    /// Iterates over all possible states accessible for the current time in reverse chronological
    /// order.
    pub fn iter_states(&self) -> Result<impl Iterator<Item = (&Timestamp, &S)>, AccessError> {
        match self.access_context {
            AccessContext::Operator => {
                Err(AccessError("Attempted to iter_states from Operator::new"))
            }
            AccessContext::Callback => Err(AccessError(
                "Attempted to iter_states from a non-watermark callback",
            )),
            AccessContext::WatermarkCallback => Ok(self
                .state_history
                .range(..=self.current_time.clone())
                .rev()
                .enumerate()
                .filter(move |x| x.0 < self.history_size)
                .map(|x| x.1)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_new_access() {
        let mut state: TimeVersionedState<usize, usize> =
            TimeVersionedState::new_with_history_size(1);
        assert_eq!(state.access_context, AccessContext::Operator);
        state.set_history_size(2).unwrap();
        assert_eq!(state.history_size(), 2);
        state.set_initial_state(99).unwrap();
        assert_eq!(Some(&99), state.state_history.get(&Timestamp::bottom()));
        assert!(state.append(3).is_err());
        assert_eq!(None, state.message_history.get(&Timestamp::bottom()));
        assert!(state.get_current_messages().is_err());
        assert!(state.get_state(&Timestamp::bottom()).is_err());
        assert!(state.get_current_state().is_err());
        assert!(state.get_current_state_mut().is_err());
        assert!(state.iter_states().is_err());
    }

    #[test]
    fn test_regular_callback_access() {
        let mut state: TimeVersionedState<usize, usize> =
            TimeVersionedState::new_with_history_size(1);
        state.access_context = AccessContext::Callback;
        let current_time = Timestamp::new(vec![1]);
        state.set_current_time(current_time.clone());
        assert!(state.set_history_size(2).is_err());
        assert_ne!(state.history_size(), 2);
        assert!(state.set_initial_state(99).is_err());
        assert_eq!(None, state.state_history.get(&Timestamp::bottom()));
        assert!(state.append(3).is_ok());
        assert_eq!(Some(&vec![3]), state.message_history.get(&current_time));
        assert!(state.get_current_messages().is_err());
        assert!(state.get_state(&Timestamp::bottom()).is_err());
        assert!(state.get_current_state().is_err());
        assert!(state.get_current_state_mut().is_err());
        assert!(state.iter_states().is_err());
    }

    #[test]
    fn test_watermark_callback_access() {
        let mut state: TimeVersionedState<usize, usize> =
            TimeVersionedState::new_with_history_size(1);
        state.set_current_time(Timestamp::new(vec![1]));
        state.access_context = AccessContext::WatermarkCallback;
        assert!(state.set_history_size(2).is_err());
        assert_ne!(state.history_size(), 2);
        assert!(state.set_initial_state(99).is_err());
        assert_eq!(None, state.state_history.get(&Timestamp::bottom()));
        assert!(state.append(3).is_err());
        assert_eq!(None, state.message_history.get(&Timestamp::bottom()));
        assert_eq!(Ok(&vec![]), state.get_current_messages());
        assert_eq!(Ok(None), state.get_state(&Timestamp::bottom()));
        assert_eq!(Ok(&usize::default()), state.get_current_state());
        assert_eq!(Ok(&mut usize::default()), state.get_current_state_mut());
        assert!(state.iter_states().is_ok());
    }

    #[test]
    /// Create TimeVersioned state with history size of 2.
    /// Set initial state.
    /// Simulate 2 callbacks which adds messages and 1 watermark callback
    /// which sums messages to produce state.
    /// Closes time and checks that GC was performed properly.
    fn test_lifecycle() {
        let mut versioned_state = TimeVersionedState::new_with_history_size(2);
        versioned_state.set_initial_state(100).unwrap();
        // Called internally by ERDOS.
        versioned_state.set_access_context(AccessContext::Callback);
        // Add all messages first.
        for i in 1..=5 {
            // Called internally by ERDOS.
            versioned_state.set_current_time(Timestamp::new(vec![i as u64]));
            // Add message data from simulated callback.
            versioned_state.append(i).unwrap();
            versioned_state.append(i * 2).unwrap();
            versioned_state.append(i * 3).unwrap();
        }
        // Called internally by ERDOS.
        versioned_state.set_access_context(AccessContext::WatermarkCallback);
        // Process messages and create states.
        for i in 1..=5 {
            let current_time = Timestamp::new(vec![i as u64]);
            // Called internally by ERDOS.
            versioned_state.set_current_time(current_time.clone());
            // Check that state is initiated to default.
            assert_eq!(versioned_state.get_current_state(), Ok(&usize::default()));
            // Generate new state from messages.
            let new_state = versioned_state
                .get_current_messages()
                .unwrap()
                .iter()
                .sum::<usize>();
            assert_eq!(new_state, i * 6);
            // Modify the current state.
            *versioned_state.get_current_state_mut().unwrap() = new_state;
            assert_eq!(versioned_state.get_current_state(), Ok(&new_state));
            // View history.
            let mut num_states_accessible = 0;
            let expected_num_states_accessible = if i < versioned_state.history_size() {
                i + 1
            } else {
                versioned_state.history_size()
            };
            for (j, (t, state)) in versioned_state.iter_states().unwrap().enumerate() {
                let (expected_t, expected_state) = match i - j {
                    0 => (Timestamp::bottom(), 100),
                    x => (Timestamp::new(vec![x as u64]), 6 * x),
                };
                assert_eq!(
                    t, &expected_t,
                    "i: {}, j: {}, expected_num_states: {}",
                    i, j, expected_num_states_accessible
                );
                assert_eq!(state, &expected_state);
                assert_eq!(versioned_state.get_state(t), Ok(Some(state)));
                num_states_accessible += 1;
            }
            assert_eq!(num_states_accessible, expected_num_states_accessible);
            // Try to get a future state.
            assert!(versioned_state
                .get_state(&Timestamp::new(vec![(i + 1) as u64]))
                .unwrap()
                .is_none());
            // Called internally by ERDOS.
            // Do this every other iteration to ensure this doesn't affect correctness.
            if i % 2 == 0 {
                versioned_state.close_time(&current_time);
                let expected_min_time =
                    Timestamp::new(vec![(i + 1 - expected_num_states_accessible) as u64]);
                let gcd_messages: Vec<_> = versioned_state
                    .message_history
                    .range(..expected_min_time.clone())
                    .collect();
                assert!(gcd_messages.is_empty());
                let gcd_states: Vec<_> = versioned_state
                    .state_history
                    .range(..expected_min_time.clone())
                    .collect();
                assert!(gcd_states.is_empty(), "{}, {:?}");
            }
        }
    }
}
