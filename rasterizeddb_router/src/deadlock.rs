//! The cycles in the cluster's wait-for graph (sharding-plan.md B4.2), and
//! which member of each ends: shared by the gateway's detector and the
//! shards.

use std::collections::{HashMap, HashSet};

/// The cycles of `edges` (waiter's global id, holder's), one victim each:
/// the member whose id is youngest. The gateway numbers its transactions in
/// order and ends each id with that number (`gateway-pid-n`).
pub fn victims(edges: &[(String, String)]) -> Vec<String> {
    let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
    for (a, b) in edges {
        graph.entry(a.as_str()).or_default().push(b.as_str());
    }
    let mut victims: HashSet<String> = HashSet::new();
    for &start in graph.keys() {
        // Any path back to `start` is a cycle through it.
        let mut stack: Vec<Vec<&str>> = vec![vec![start]];
        let mut seen: HashSet<&str> = HashSet::new();
        while let Some(path) = stack.pop() {
            let last = *path.last().expect("paths are never empty");
            for &next in graph.get(last).into_iter().flatten() {
                if next == start {
                    let victim = path.iter().max_by(|a, b| order(a).cmp(&order(b))).expect("a cycle has members");
                    victims.insert(victim.to_string());
                } else if seen.insert(next) {
                    let mut p = path.clone();
                    p.push(next);
                    stack.push(p);
                }
            }
        }
    }
    let mut v: Vec<String> = victims.into_iter().collect();
    v.sort();
    v
}

/// How ids compare for youth: their trailing number, then their text.
fn order(gid: &str) -> (u64, &str) {
    (gid.rsplit('-').next().and_then(|n| n.parse().ok()).unwrap_or(0), gid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(a: &str, b: &str) -> (String, String) {
        (a.to_string(), b.to_string())
    }

    #[test]
    fn b42_a_cycle_ends_its_youngest_member_and_a_chain_none() {
        assert!(victims(&[e("g-1", "g-2"), e("g-2", "g-3")]).is_empty(), "a chain is no deadlock");
        assert_eq!(victims(&[e("g-1", "g-2"), e("g-2", "g-1")]), vec!["g-2".to_string()]);
        assert_eq!(victims(&[e("g-9", "g-10"), e("g-10", "g-11"), e("g-11", "g-9")]), vec!["g-11".to_string()], "numeric, not text order");
        let v = victims(&[e("a-1", "a-2"), e("a-2", "a-1"), e("b-5", "b-6"), e("b-6", "b-5")]);
        assert_eq!(v, vec!["a-2".to_string(), "b-6".to_string()]);
        // A cycle reached from outside it ends one of its own members.
        assert_eq!(victims(&[e("x-99", "y-1"), e("y-1", "y-2"), e("y-2", "y-1")]), vec!["y-2".to_string()]);
    }
}
