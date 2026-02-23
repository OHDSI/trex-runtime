use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;

pub fn topological_sort(
    nodes: &[String],
    edges: &HashMap<String, HashSet<String>>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let node_set: HashSet<&String> = nodes.iter().collect();

    let mut in_degree: HashMap<&String, usize> = HashMap::new();
    for node in nodes {
        in_degree.insert(node, 0);
    }

    // Invert edges: dependency -> list of dependents
    let mut forward: HashMap<&String, Vec<&String>> = HashMap::new();
    for node in nodes {
        forward.insert(node, Vec::new());
    }

    for (node, deps) in edges {
        if !node_set.contains(node) {
            continue;
        }
        for dep in deps {
            if node_set.contains(dep) {
                forward.get_mut(dep).unwrap().push(node);
                *in_degree.get_mut(node).unwrap() += 1;
            }
        }
    }

    let mut queue: VecDeque<&String> = VecDeque::new();
    for (node, &degree) in &in_degree {
        if degree == 0 {
            queue.push_back(node);
        }
    }

    let mut sorted_queue: Vec<&String> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut result = Vec::new();

    while let Some(node) = queue.pop_front() {
        result.push(node.clone());

        let mut next_nodes: Vec<&String> = Vec::new();
        for dependent in &forward[node] {
            let deg = in_degree.get_mut(dependent).unwrap();
            *deg -= 1;
            if *deg == 0 {
                next_nodes.push(dependent);
            }
        }
        next_nodes.sort();
        queue.extend(next_nodes);
    }

    if result.len() != nodes.len() {
        let missing: Vec<String> = nodes
            .iter()
            .filter(|n| !result.contains(n))
            .cloned()
            .collect();
        return Err(format!("Circular dependency detected involving: {}", missing.join(", ")).into());
    }

    Ok(result)
}

pub fn transitive_dependents(
    changed: &HashSet<String>,
    all_nodes: &[String],
    edges: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    let mut reverse: HashMap<&String, Vec<&String>> = HashMap::new();
    for node in all_nodes {
        reverse.insert(node, Vec::new());
    }
    for (node, deps) in edges {
        for dep in deps {
            if let Some(dependents) = reverse.get_mut(dep) {
                dependents.push(node);
            }
        }
    }

    let mut affected: HashSet<String> = changed.clone();
    let mut queue: VecDeque<String> = changed.iter().cloned().collect();

    while let Some(node) = queue.pop_front() {
        if let Some(dependents) = reverse.get(&node) {
            for dep in dependents {
                if affected.insert((*dep).clone()) {
                    queue.push_back((*dep).clone());
                }
            }
        }
    }

    affected
}
