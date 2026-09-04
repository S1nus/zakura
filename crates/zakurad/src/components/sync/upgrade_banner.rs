//! Network upgrade activation announcements.

use zakura_chain::{
    block::Height,
    chain_tip::ChainTip,
    parameters::{Network, NetworkUpgrade},
};

use crate::BoxError;

/// Logs a banner whenever the best chain crosses a network upgrade activation height.
pub async fn show_network_upgrade_banners(
    network: Network,
    mut latest_chain_tip: impl ChainTip,
) -> Result<(), BoxError> {
    let mut previous_height = latest_chain_tip.best_tip_height();

    loop {
        latest_chain_tip.best_tip_changed().await?;

        let current_height = latest_chain_tip.best_tip_height();
        if let Some(current_height) = current_height {
            for (activation_height, network_upgrade) in
                crossed_network_upgrades(&network, previous_height, current_height)
            {
                tracing::info!(
                    ?network,
                    %network_upgrade,
                    ?activation_height,
                    "{}",
                    network_upgrade_banner(network_upgrade, activation_height),
                );
            }
        }

        previous_height = current_height;
    }
}

fn crossed_network_upgrades(
    network: &Network,
    previous_height: Option<Height>,
    current_height: Height,
) -> Vec<(Height, NetworkUpgrade)> {
    network
        .activation_list()
        .into_iter()
        .filter(|(activation_height, _)| {
            previous_height.is_none_or(|previous_height| *activation_height > previous_height)
                && *activation_height <= current_height
        })
        .collect()
}

fn network_upgrade_banner(network_upgrade: NetworkUpgrade, activation_height: Height) -> String {
    let activation_height = activation_height.0;

    format!(
        "\n\
============================================================\n\
🚀  ZAKURA NETWORK UPGRADE ACTIVATED: {network_upgrade}  🚀\n\
⛓️  Activation height: {activation_height}\n\
============================================================"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::parameters::NetworkUpgrade::{Blossom, Canopy, Genesis, Heartwood, Sapling};

    #[test]
    fn announces_every_upgrade_crossed_by_a_tip_change() {
        let network = Network::Mainnet;
        let sapling_height = Sapling
            .activation_height(&network)
            .expect("Mainnet Sapling activation height is configured");
        let canopy_height = Canopy
            .activation_height(&network)
            .expect("Mainnet Canopy activation height is configured");

        assert_eq!(
            crossed_network_upgrades(&network, Some(sapling_height), canopy_height),
            vec![
                (
                    Blossom
                        .activation_height(&network)
                        .expect("Mainnet Blossom activation height is configured"),
                    Blossom,
                ),
                (
                    Heartwood
                        .activation_height(&network)
                        .expect("Mainnet Heartwood activation height is configured"),
                    Heartwood,
                ),
                (canopy_height, Canopy),
            ]
        );
    }

    #[test]
    fn announces_genesis_when_the_first_tip_arrives() {
        assert_eq!(
            crossed_network_upgrades(&Network::Mainnet, None, Height(0)),
            vec![(Height(0), Genesis)]
        );
    }

    #[test]
    fn does_not_announce_while_the_tip_moves_backward() {
        let network = Network::Mainnet;
        let sapling_height = Sapling
            .activation_height(&network)
            .expect("Mainnet Sapling activation height is configured");
        let canopy_height = Canopy
            .activation_height(&network)
            .expect("Mainnet Canopy activation height is configured");

        assert!(crossed_network_upgrades(&network, Some(canopy_height), sapling_height).is_empty());
    }

    #[test]
    fn banner_contains_the_upgrade_and_height() {
        let banner = network_upgrade_banner(Canopy, Height(1_046_400));

        assert!(banner.contains("ZAKURA NETWORK UPGRADE ACTIVATED: Canopy"));
        assert!(banner.contains("Activation height: 1046400"));
    }
}
