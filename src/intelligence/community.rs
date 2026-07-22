use nalgebra::{DMatrix, SymmetricEigen};
use std::collections::HashMap;

/// Spectral clustering over the knowledge graph. See VENTURI_ROADMAP.md R1.
///
/// `graph_query()`'s BFS traversal is local and greedy — it only finds
/// concepts within a few hops of the query's anchor nodes. Two concepts can
/// belong to the same semantic community while sitting many hops apart, and
/// BFS never reaches them. Spectral clustering looks at the whole graph at
/// once (via the normalized Laplacian's eigenvectors) and assigns every node
/// a `community_id`, so `KnowledgeGraph::traverse()` can add a hop-independent
/// "same community" pass on top of BFS.
pub struct CommunityDetector {
    max_k: usize,
}

impl CommunityDetector {
    /// `max_k` is the upper bound on cluster count (roadmap: 5-10). The
    /// detector uses `min(max_k, node_count / 2)` so it never asks for more
    /// clusters than the graph can meaningfully support.
    pub fn new(max_k: usize) -> Self {
        Self { max_k }
    }

    /// `nodes` defines the matrix ordering. `edges` is `(from, to, weight)`,
    /// undirected — a pair may appear once; the reverse direction is implied.
    ///
    /// Returns `node_id -> community_id` (`"c0"..="c{k-1}"`). Nodes with no
    /// edges (degree 0) can't be placed by spectral coordinates and are
    /// omitted from the result. Returns an empty map when there isn't enough
    /// graph structure to cluster (fewer than `2 * k` nodes, or no edges).
    pub fn detect(
        &self,
        nodes: &[String],
        edges: &[(String, String, f64)],
    ) -> HashMap<String, String> {
        let n = nodes.len();
        let k = self.max_k.min(n / 2);
        if k < 2 {
            return HashMap::new();
        }

        let index: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();

        let mut adjacency = DMatrix::<f64>::zeros(n, n);
        for (from, to, weight) in edges {
            if let (Some(&i), Some(&j)) = (index.get(from.as_str()), index.get(to.as_str())) {
                if i != j && *weight > 0.0 {
                    adjacency[(i, j)] += weight;
                    adjacency[(j, i)] += weight;
                }
            }
        }

        let degrees: Vec<f64> = (0..n).map(|i| adjacency.row(i).sum()).collect();
        if degrees.iter().all(|&d| d == 0.0) {
            return HashMap::new();
        }

        // Normalized Laplacian: L = I - D^(-1/2) A D^(-1/2)
        let mut laplacian = DMatrix::<f64>::identity(n, n);
        for i in 0..n {
            if degrees[i] == 0.0 {
                continue;
            }
            for j in 0..n {
                let a_ij = adjacency[(i, j)];
                if a_ij != 0.0 && degrees[j] > 0.0 {
                    laplacian[(i, j)] -= a_ij / (degrees[i].sqrt() * degrees[j].sqrt());
                }
            }
        }

        let eigen = SymmetricEigen::new(laplacian);
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| {
            eigen.eigenvalues[a]
                .partial_cmp(&eigen.eigenvalues[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // Skip the smallest eigenvalue (the trivial constant eigenvector) and
        // take the next `k - 1` as the spectral embedding coordinates — the
        // standard spectral-clustering convention (Ng-Jordan-Weiss: k total
        // eigenvectors including the constant one, which contributes zero
        // variance and is therefore free to include or skip). Using a full
        // `k` non-trivial dimensions for a `k`-way split over-provisions by
        // one axis, which just adds clustering noise without adding
        // separating signal.
        let coord_indices: Vec<usize> = order.into_iter().skip(1).take(k - 1).collect();
        if coord_indices.is_empty() {
            return HashMap::new();
        }

        let coords: Vec<Vec<f64>> = (0..n)
            .map(|i| {
                coord_indices
                    .iter()
                    .map(|&c| eigen.eigenvectors[(i, c)])
                    .collect()
            })
            .collect();

        let assignments = kmeans(&coords, k);

        nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| degrees[*i] > 0.0)
            .map(|(i, id)| (id.clone(), format!("c{}", assignments[i])))
            .collect()
    }
}

/// Deterministic k-means over the spectral coordinates. Initial centroids are
/// chosen by sorting on the first coordinate and taking `k` evenly spaced
/// rows — deterministic seeding keeps community assignment reproducible for
/// tests and avoids pulling in an RNG crate for one clustering pass.
fn kmeans(coords: &[Vec<f64>], k: usize) -> Vec<usize> {
    let n = coords.len();
    let dims = coords[0].len();

    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        coords[a][0]
            .partial_cmp(&coords[b][0])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut centroids: Vec<Vec<f64>> = (0..k).map(|c| coords[order[c * n / k]].clone()).collect();

    let mut assignments = vec![0usize; n];
    for _ in 0..50 {
        let mut changed = false;
        for i in 0..n {
            let mut best = 0;
            let mut best_dist = f64::MAX;
            for (c, centroid) in centroids.iter().enumerate() {
                let dist: f64 = (0..dims)
                    .map(|d| (coords[i][d] - centroid[d]).powi(2))
                    .sum();
                if dist < best_dist {
                    best_dist = dist;
                    best = c;
                }
            }
            if assignments[i] != best {
                changed = true;
            }
            assignments[i] = best;
        }
        if !changed {
            break;
        }
        for (c, centroid) in centroids.iter_mut().enumerate() {
            let members: Vec<usize> = (0..n).filter(|&i| assignments[i] == c).collect();
            if members.is_empty() {
                continue;
            }
            for d in 0..dims {
                centroid[d] =
                    members.iter().map(|&i| coords[i][d]).sum::<f64>() / members.len() as f64;
            }
        }
    }
    assignments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(a: &str, b: &str, w: f64) -> (String, String, f64) {
        (a.to_string(), b.to_string(), w)
    }

    #[test]
    fn detect_groups_two_clusters_joined_by_a_weak_bridge() {
        let nodes: Vec<String> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // Two triangles joined by one far-weaker bridge edge. Kept connected
        // (rather than fully disjoint) on purpose: a literally disconnected
        // graph has a degenerate multi-dimensional zero-eigenspace, and the
        // eigensolver's arbitrary basis for it doesn't have to align with
        // component boundaries. A single weak bridge keeps the zero
        // eigenvalue non-degenerate (graph has one connected component) so
        // the Fiedler vector is well-defined and approximates the min cut —
        // the realistic case for Venturi's graph too (nothing is ever
        // literally disconnected once enough chains have been ingested).
        let edges = vec![
            edge("a", "b", 1.0),
            edge("b", "c", 1.0),
            edge("a", "c", 1.0),
            edge("d", "e", 1.0),
            edge("e", "f", 1.0),
            edge("d", "f", 1.0),
            edge("c", "d", 0.01),
        ];

        let detector = CommunityDetector::new(2);
        let assignments = detector.detect(&nodes, &edges);

        assert_eq!(assignments.len(), 6);
        // "a" and "f" sit deepest inside their respective triangles, farthest
        // from the bridge — the clearest signal spectral clustering has to
        // work with. Nodes "c"/"d" carry the bridge edge itself and can land
        // on either side of a k-means boundary; that's expected spectral
        // clustering behavior at a community's edge, not asserted here.
        assert_eq!(assignments["a"], assignments["b"]);
        assert_eq!(assignments["e"], assignments["f"]);
        assert_ne!(assignments["a"], assignments["f"]);
    }

    #[test]
    fn detect_returns_empty_when_graph_too_small_for_k() {
        let nodes: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let edges = vec![edge("a", "b", 1.0)];

        // max_k=2 needs at least 4 nodes (n/2 >= 2); only 3 given.
        let detector = CommunityDetector::new(2);
        assert!(detector.detect(&nodes, &edges).is_empty());
    }

    #[test]
    fn detect_omits_isolated_nodes_with_no_edges() {
        let nodes: Vec<String> = ["a", "b", "c", "d", "isolated"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let edges = vec![
            edge("a", "b", 1.0),
            edge("b", "c", 1.0),
            edge("c", "d", 1.0),
            edge("a", "d", 1.0),
        ];

        let detector = CommunityDetector::new(2);
        let assignments = detector.detect(&nodes, &edges);

        assert!(!assignments.contains_key("isolated"));
        assert_eq!(assignments.len(), 4);
    }
}
