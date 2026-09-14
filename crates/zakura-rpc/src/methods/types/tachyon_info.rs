//! Response types for the `gettachyoninfo` RPC.

use derive_getters::Getters;
use zakura_chain::block::Height;

/// The current selected chain's Tachyon accumulator and retention state.
#[derive(Clone, Debug, Eq, Getters, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTachyonInfoResponse {
    /// The current best-chain tip height.
    #[getter(copy)]
    height: Height,

    /// The NuTachyon activation height.
    #[getter(copy)]
    activation_height: Height,

    /// The current Tachyon epoch, or `None` before NuTachyon activation.
    #[getter(copy)]
    epoch: Option<u32>,

    /// The number of blocks in a Tachyon epoch.
    #[getter(copy)]
    epoch_length: u32,

    /// The first block height of the next Tachyon epoch.
    #[getter(copy)]
    next_epoch_height: Height,

    /// The Tachyon accumulator after the current best-chain tip.
    #[serde(with = "hex")]
    #[getter(copy)]
    tip_anchor: [u8; 32],

    /// The number of Tachygrams retained in the current two-epoch scan window.
    #[getter(copy)]
    retained_tachygram_count: usize,

    /// The logical payload size of the retained Tachygrams.
    #[getter(copy)]
    tachygram_payload_bytes: u64,

    /// A bounded sample of the most recently revealed retained Tachygrams.
    recent_tachygrams: Vec<RetainedTachygram>,
}

/// A Tachygram retained in the selected chain's current scan window.
#[derive(Clone, Debug, Eq, Getters, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetainedTachygram {
    /// The Tachygram in display byte order.
    #[serde(with = "hex")]
    #[getter(copy)]
    tachygram: [u8; 32],

    /// The best-chain height that revealed the Tachygram.
    #[getter(copy)]
    revealed_height: Height,
}

impl GetTachyonInfoResponse {
    pub(crate) fn from_state(
        network: &zakura_chain::parameters::Network,
        state: zakura_state::response::TachyonPoolState,
    ) -> Option<Self> {
        use zakura_chain::parameters::NetworkUpgrade;

        let activation_height = NetworkUpgrade::NuTachyon.activation_height(network)?;
        let epoch = zakura_chain::tachyon::epoch(network, state.tip_height);
        let next_epoch_index = epoch.map_or(1, |epoch| epoch.saturating_add(1));
        let next_epoch_offset =
            next_epoch_index.checked_mul(zakura_chain::tachyon::EPOCH_LENGTH)?;
        let next_epoch_height = Height(activation_height.0.checked_add(next_epoch_offset)?);
        let retained_tachygram_count = state.retained_tachygram_count;
        let tachygram_payload_bytes = u64::try_from(retained_tachygram_count)
            .expect("a Tachygram count fits in u64")
            .saturating_mul(32);
        let recent_tachygrams = state
            .recent_tachygrams
            .into_iter()
            .map(|(tachygram, revealed_height)| RetainedTachygram {
                tachygram: tachygram.0,
                revealed_height,
            })
            .collect();

        Some(Self {
            height: state.tip_height,
            activation_height,
            epoch,
            epoch_length: zakura_chain::tachyon::EPOCH_LENGTH,
            next_epoch_height,
            tip_anchor: state.tip_anchor.0,
            retained_tachygram_count,
            tachygram_payload_bytes,
            recent_tachygrams,
        })
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{
        block::Height,
        parameters::{testnet::ConfiguredActivationHeights, Network},
        tachyon::{Anchor, Tachygram},
    };

    use super::*;

    #[test]
    fn response_reports_epoch_boundary_and_payload_size() {
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                canopy: Some(1),
                nu5: Some(2),
                nu6: Some(3),
                nu6_1: Some(4),
                nu6_2: Some(5),
                nu6_3: Some(6),
                nu_tachyon: Some(7),
                ..Default::default()
            }
            .into(),
        );
        let state = zakura_state::response::TachyonPoolState {
            tip_height: Height(8),
            tip_anchor: Anchor([1; 32]),
            retained_tachygram_count: 3,
            recent_tachygrams: vec![(Tachygram([2; 32]), Height(8))],
        };

        let response = GetTachyonInfoResponse::from_state(&network, state).unwrap();

        assert_eq!(response.epoch(), Some(0));
        assert_eq!(
            response.next_epoch_height(),
            Height(7 + response.epoch_length())
        );
        assert_eq!(response.tachygram_payload_bytes(), 96);
        assert_eq!(response.tip_anchor(), [1; 32]);
        assert_eq!(response.recent_tachygrams()[0].tachygram(), [2; 32]);
    }
}
