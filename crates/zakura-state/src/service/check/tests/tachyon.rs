//! Mempool scan-window regression tests, independent of proof verification.

use crate::{
    arbitrary::Prepare,
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::{
        check::{anchors, nullifier},
        finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
        non_finalized_state::Chain,
    },
    Config, ValidateContextError,
};
use std::{collections::BTreeSet, sync::Arc};
use zakura_chain::{
    amount::Amount,
    block::{Block, Height},
    parameters::{testnet::ConfiguredActivationHeights, Network, NetworkUpgrade},
    serialization::ZcashDeserializeInto,
    tachyon,
    transaction::{LockTime, Transaction, UnminedTx},
    value_balance::ValueBalance,
};
use zcash_tachyon::{
    bundle::Signature, Anchor, Bundle, ProofStamp, Tachygram, TachygramSetPoly, TachyonBundle,
};

#[test]
fn tachyon_mempool_context_enforces_epoch_boundary() {
    let _guard = zakura_test::init();
    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            canopy: Some(1),
            nu5: Some(2),
            nu6: Some(3),
            nu6_1: Some(4),
            nu6_2: Some(5),
            nu6_3: Some(6),
            nu7: Some(8),
            nu_tachyon: Some(10),
            ..Default::default()
        }
        .into(),
    );
    let db = ZakuraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .unwrap();
    let anchor = Anchor::read(&[0; 32][..]).unwrap();
    let gram = Tachygram::read(&[0; 32][..]).unwrap();
    let grams = BTreeSet::from([gram]);
    let bundle = Bundle {
        value_balance: 0i64.try_into().unwrap(),
        actions: Vec::new(),
        binding_sig: Signature::read(&[0; 64][..]).unwrap(),
        memo: Vec::new(),
        stamp: ProofStamp {
            coverage: [0; 32],
            anchor,
            tachygram_set: grams.iter().copied().collect::<TachygramSetPoly>().commit(),
            tachygrams: grams,
            proof: Box::new(ragu::Proof::trivial()),
        },
    };
    let tx: UnminedTx = Transaction::V7 {
        network_upgrade: NetworkUpgrade::NuTachyon,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        zip233_amount: Amount::zero(),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        tachyon_shielded_data: Some(TachyonBundle::Proven(bundle).into()),
    }
    .into();
    let mut chain = Chain::new(
        &network,
        Height(0),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::zero(),
    );
    // Only the tip-height and Tachyon indexes are consulted by these checks.
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let indexed = Arc::new(block.prepare().test_with_zero_spent_utxos());
    let boundary = 10 + 2 * tachyon::EPOCH_LENGTH;
    chain.blocks.insert(Height(boundary - 2), indexed.clone());
    chain
        .tachyon_anchors
        .insert(anchor.into(), vec![Height(10)]);
    chain
        .tachyon_tachygrams
        .insert(gram.into(), vec![Height(10)]);
    let mut chain = Arc::new(chain);
    anchors::tx_anchors_refer_to_final_treestates(&db, Some(&chain), &tx)
        .expect("epoch zero is still in the candidate's previous epoch");
    assert!(matches!(
        nullifier::tx_no_duplicates_in_chain(&db, Some(&chain), tx.transaction()),
        Err(ValidateContextError::DuplicateTachyonTachygram { .. })
    ));

    Arc::make_mut(&mut chain)
        .blocks
        .insert(Height(boundary - 1), indexed);
    assert!(matches!(
        anchors::tx_anchors_refer_to_final_treestates(&db, Some(&chain), &tx),
        Err(ValidateContextError::UnknownTachyonAnchor { .. })
    ));
    nullifier::tx_no_duplicates_in_chain(&db, Some(&chain), tx.transaction())
        .expect("epoch-zero revelations have left the two-epoch scan window");
}
