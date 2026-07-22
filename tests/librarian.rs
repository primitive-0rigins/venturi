#[test]
fn structured_filter_defaults_to_empty() {
    let filter = venturi::StructuredFilter::default();

    assert!(filter.topic.is_none());
    assert!(filter.domain.is_none());
    assert!(filter.tier.is_none());
    assert!(filter.parent_id.is_none());
    assert!(filter.format.is_none());
    assert!(filter.classification.is_none());
    assert!(filter.date_from.is_none());
    assert!(filter.date_to.is_none());
}
