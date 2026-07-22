use venturi::{OrbShelf, RetrievalPipeline, WormholeTunnel};

/// Full roundtrip: seal → store → load → rehydrate → verify original content.
#[test]
fn seal_store_load_rehydrate() {
    let content = b"Venturi - regulated agent memory infrastructure.";
    let parent_id = "test-parent-0001".to_string();

    let tunnel = WormholeTunnel::new();
    let (orb, chain_key) = tunnel
        .seal_chunk(content.to_vec(), parent_id.clone(), 1, 1)
        .expect("seal_chunk failed");

    let orb_id = orb.id.clone();
    assert_eq!(orb.sequence, 1);
    assert_eq!(orb.chain_length, 1);
    assert_eq!(orb.parent_id, parent_id);

    let shelf = OrbShelf::new("/tmp/venturi-test-shelf");
    shelf.store(&orb).expect("store failed");
    assert!(shelf.exists(&orb_id));

    let loaded = shelf.load(&orb_id, parent_id).expect("load failed");
    assert_eq!(loaded.id, orb_id);

    let pipeline = RetrievalPipeline::new();
    let recovered = pipeline
        .rehydrate(loaded, &chain_key)
        .expect("rehydrate failed");
    assert_eq!(recovered.as_slice(), content.as_ref());
}

/// Two-orb chain: parent_id and sequence survive shelf roundtrip.
#[test]
fn chain_roundtrip() {
    let chunks: &[&[u8]] = &[b"chunk one content", b"chunk two content"];
    let parent_id = "test-chain-0001".to_string();
    let tunnel = WormholeTunnel::new();
    let pipeline = RetrievalPipeline::new();
    let shelf = OrbShelf::new("/tmp/venturi-test-shelf-chain");

    let mut orb_ids = Vec::new();
    let mut keys: Vec<[u8; 32]> = Vec::new();

    for (i, chunk) in chunks.iter().enumerate() {
        let (orb, key) = tunnel
            .seal_chunk(chunk.to_vec(), parent_id.clone(), (i + 1) as u32, 2)
            .expect("seal_chunk failed");
        assert_eq!(orb.sequence, (i + 1) as u32);
        assert_eq!(orb.chain_length, 2);
        orb_ids.push(orb.id.clone());
        keys.push(key);
        shelf.store(&orb).expect("store failed");
    }

    let mut reassembled: Vec<Vec<u8>> = Vec::new();
    for (orb_id, key) in orb_ids.iter().zip(keys.iter()) {
        let orb = shelf.load(orb_id, parent_id.clone()).expect("load failed");
        let content = pipeline.rehydrate(orb, key).expect("rehydrate failed");
        reassembled.push(content);
    }

    assert_eq!(reassembled[0].as_slice(), b"chunk one content".as_ref());
    assert_eq!(reassembled[1].as_slice(), b"chunk two content".as_ref());
}
