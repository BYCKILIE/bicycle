//! Job graph optimizer.
//!
//! This module provides optimization passes for job graphs, including:
//! - Operator chaining: Fuse operators into single tasks to reduce overhead
//! - Slot sharing: Group operators to share task slots efficiently
//!
//! # Operator Chaining
//!
//! Operators can be chained when:
//! - They have the same parallelism
//! - They are connected via forward partitioning
//! - Neither operator has disabled chaining
//! - They are in the same slot sharing group
//!
//! ```text
//! Before optimization:
//!   [Source(p=2)] -> [Map(p=2)] -> [Filter(p=2)] -> [KeyBy(p=4)] -> [Reduce(p=4)] -> [Sink(p=4)]
//!   Total tasks: 2 + 2 + 2 + 4 + 4 + 4 = 18
//!
//! After optimization:
//!   [Source->Map->Filter(p=2)] -> [KeyBy->Reduce->Sink(p=4)]
//!   Total tasks: 2 + 4 = 6
//! ```

use crate::graph::{Edge, JobGraph, OperatorType, PartitionStrategy, Vertex};
use std::collections::{HashMap, HashSet};

/// Optimizer configuration.
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Enable operator chaining optimization.
    pub chaining_enabled: bool,
    /// Enable slot sharing optimization.
    pub slot_sharing_enabled: bool,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            chaining_enabled: true,
            slot_sharing_enabled: true,
        }
    }
}

/// A chain of operators that will be executed as a single task.
#[derive(Debug, Clone)]
pub struct OperatorChain {
    /// Chain ID (uses the ID of the first operator).
    pub id: String,
    /// Combined name of all operators in the chain.
    pub name: String,
    /// UIDs of operators in the chain (for state recovery).
    pub operator_uids: Vec<Option<String>>,
    /// IDs of operators in this chain (in execution order).
    pub operator_ids: Vec<String>,
    /// The operators in this chain (in execution order).
    pub operators: Vec<Vertex>,
    /// Parallelism of the chain.
    pub parallelism: u32,
    /// Max parallelism of the chain.
    pub max_parallelism: Option<u32>,
    /// Slot sharing group.
    pub slot_sharing_group: String,
    /// Whether the chain head is a source.
    pub is_source: bool,
    /// Whether the chain tail is a sink.
    pub is_sink: bool,
}

impl OperatorChain {
    /// Create a new chain with a single operator.
    pub fn new(vertex: Vertex) -> Self {
        let is_source = matches!(vertex.operator_type, OperatorType::Source { .. });
        let is_sink = matches!(vertex.operator_type, OperatorType::Sink { .. });

        Self {
            id: vertex.id.clone(),
            name: vertex.name.clone(),
            operator_uids: vec![vertex.uid.clone()],
            operator_ids: vec![vertex.id.clone()],
            parallelism: vertex.parallelism,
            max_parallelism: vertex.max_parallelism,
            slot_sharing_group: vertex.slot_sharing_group.clone(),
            is_source,
            is_sink,
            operators: vec![vertex],
        }
    }

    /// Try to append an operator to this chain.
    /// Returns true if successful, false if the operator cannot be chained.
    pub fn try_append(&mut self, vertex: Vertex, edge: &Edge) -> bool {
        // Check chaining conditions
        if !self.can_chain_with(&vertex, edge) {
            return false;
        }

        // Update chain properties
        self.name = format!("{} -> {}", self.name, vertex.name);
        self.operator_uids.push(vertex.uid.clone());
        self.operator_ids.push(vertex.id.clone());
        self.is_sink = matches!(vertex.operator_type, OperatorType::Sink { .. });

        // Take the more restrictive max_parallelism
        if let Some(mp) = vertex.max_parallelism {
            self.max_parallelism = Some(
                self.max_parallelism.map(|m| m.min(mp)).unwrap_or(mp)
            );
        }

        self.operators.push(vertex);
        true
    }

    /// Check if an operator can be chained to this chain.
    fn can_chain_with(&self, vertex: &Vertex, edge: &Edge) -> bool {
        // Must have forward partitioning
        if edge.partition_strategy != PartitionStrategy::Forward {
            return false;
        }

        // Must have same parallelism
        if vertex.parallelism != self.parallelism {
            return false;
        }

        // Both must have chaining enabled
        let tail = self.operators.last().unwrap();
        if !tail.chaining_enabled || !vertex.chaining_enabled {
            return false;
        }

        // Must be in the same slot sharing group
        if vertex.slot_sharing_group != self.slot_sharing_group {
            return false;
        }

        // Cannot chain after a sink
        if self.is_sink {
            return false;
        }

        // Cannot chain a source (sources must be chain heads)
        if matches!(vertex.operator_type, OperatorType::Source { .. }) {
            return false;
        }

        true
    }

    /// Get the total number of tasks for this chain.
    pub fn task_count(&self) -> u32 {
        self.parallelism
    }
}

/// Optimized job graph with operator chains.
#[derive(Debug, Clone)]
pub struct OptimizedJobGraph {
    /// Original job name.
    pub name: String,
    /// Operator chains (each chain becomes a task).
    pub chains: Vec<OperatorChain>,
    /// Edges between chains.
    pub chain_edges: Vec<ChainEdge>,
    /// Original job config.
    pub config: crate::graph::JobConfig,
    /// Slot sharing groups and their chains.
    pub slot_sharing_groups: HashMap<String, Vec<usize>>,
    /// Statistics about the optimization.
    pub stats: OptimizationStats,
}

/// Edge between operator chains.
#[derive(Debug, Clone)]
pub struct ChainEdge {
    /// Source chain ID.
    pub source_chain_id: String,
    /// Target chain ID.
    pub target_chain_id: String,
    /// Partition strategy.
    pub partition_strategy: PartitionStrategy,
}

/// Statistics about the optimization.
#[derive(Debug, Clone, Default)]
pub struct OptimizationStats {
    /// Number of operators before optimization.
    pub operators_before: usize,
    /// Number of chains after optimization.
    pub chains_after: usize,
    /// Number of tasks before optimization (sum of all operator parallelisms).
    pub tasks_before: u32,
    /// Number of tasks after optimization (sum of all chain parallelisms).
    pub tasks_after: u32,
    /// Number of slot sharing groups.
    pub slot_sharing_groups: usize,
    /// Estimated minimum slots needed.
    pub min_slots_needed: u32,
}

impl OptimizationStats {
    /// Calculate task reduction percentage.
    pub fn task_reduction_percent(&self) -> f64 {
        if self.tasks_before == 0 {
            0.0
        } else {
            (1.0 - (self.tasks_after as f64 / self.tasks_before as f64)) * 100.0
        }
    }
}

/// Job graph optimizer.
pub struct JobGraphOptimizer {
    config: OptimizerConfig,
}

impl JobGraphOptimizer {
    /// Create a new optimizer with default configuration.
    pub fn new() -> Self {
        Self {
            config: OptimizerConfig::default(),
        }
    }

    /// Create a new optimizer with custom configuration.
    pub fn with_config(config: OptimizerConfig) -> Self {
        Self { config }
    }

    /// Optimize a job graph.
    pub fn optimize(&self, graph: &JobGraph) -> OptimizedJobGraph {
        let mut stats = OptimizationStats {
            operators_before: graph.vertices.len(),
            tasks_before: graph.vertices.iter().map(|v| v.parallelism).sum(),
            ..Default::default()
        };

        // Build adjacency lists
        let (downstream, upstream) = self.build_adjacency_lists(graph);

        // Find chain heads (sources or operators with non-forward input or different parallelism)
        let chain_heads = self.find_chain_heads(graph, &upstream);

        // Build chains starting from each head
        let chains = self.build_chains(graph, &chain_heads, &downstream);

        // Build edges between chains
        let chain_edges = self.build_chain_edges(graph, &chains);

        // Group chains by slot sharing group
        let slot_sharing_groups = self.group_by_slot_sharing(&chains);

        // Calculate statistics
        stats.chains_after = chains.len();
        stats.tasks_after = chains.iter().map(|c| c.task_count()).sum();
        stats.slot_sharing_groups = slot_sharing_groups.len();
        stats.min_slots_needed = self.calculate_min_slots(&chains, &slot_sharing_groups);

        OptimizedJobGraph {
            name: graph.name.clone(),
            chains,
            chain_edges,
            config: graph.config.clone(),
            slot_sharing_groups,
            stats,
        }
    }

    /// Build adjacency lists for the graph.
    fn build_adjacency_lists(&self, graph: &JobGraph) -> (HashMap<String, Vec<(String, Edge)>>, HashMap<String, Vec<(String, Edge)>>) {
        let mut downstream: HashMap<String, Vec<(String, Edge)>> = HashMap::new();
        let mut upstream: HashMap<String, Vec<(String, Edge)>> = HashMap::new();

        for edge in &graph.edges {
            downstream
                .entry(edge.source_id.clone())
                .or_default()
                .push((edge.target_id.clone(), edge.clone()));
            upstream
                .entry(edge.target_id.clone())
                .or_default()
                .push((edge.source_id.clone(), edge.clone()));
        }

        (downstream, upstream)
    }

    /// Find operators that must be chain heads.
    fn find_chain_heads(&self, graph: &JobGraph, upstream: &HashMap<String, Vec<(String, Edge)>>) -> HashSet<String> {
        let vertex_map: HashMap<_, _> = graph.vertices.iter()
            .map(|v| (v.id.clone(), v))
            .collect();

        let mut heads = HashSet::new();

        for vertex in &graph.vertices {
            let is_head = if let Some(inputs) = upstream.get(&vertex.id) {
                if inputs.is_empty() {
                    // No inputs - this is a source, definitely a head
                    true
                } else if inputs.len() > 1 {
                    // Multiple inputs - must be a head (joins, unions)
                    true
                } else {
                    // Single input - check if chainable
                    let (upstream_id, edge) = &inputs[0];
                    let upstream_vertex = vertex_map.get(upstream_id).unwrap();

                    // Not chainable if:
                    // - Different parallelism
                    // - Non-forward partitioning
                    // - Chaining disabled on either operator
                    // - Different slot sharing groups
                    edge.partition_strategy != PartitionStrategy::Forward
                        || vertex.parallelism != upstream_vertex.parallelism
                        || !vertex.chaining_enabled
                        || !upstream_vertex.chaining_enabled
                        || vertex.slot_sharing_group != upstream_vertex.slot_sharing_group
                }
            } else {
                // No upstream info - treat as head
                true
            };

            if is_head {
                heads.insert(vertex.id.clone());
            }
        }

        heads
    }

    /// Build operator chains.
    fn build_chains(
        &self,
        graph: &JobGraph,
        chain_heads: &HashSet<String>,
        downstream: &HashMap<String, Vec<(String, Edge)>>,
    ) -> Vec<OperatorChain> {
        let vertex_map: HashMap<_, _> = graph.vertices.iter()
            .map(|v| (v.id.clone(), v.clone()))
            .collect();

        let mut chains = Vec::new();
        let mut vertex_to_chain: HashMap<String, usize> = HashMap::new();

        // Process each chain head
        for head_id in chain_heads {
            let head_vertex = vertex_map.get(head_id).unwrap().clone();
            let mut chain = OperatorChain::new(head_vertex);

            // Follow the chain downstream
            let mut current_id = head_id.clone();
            loop {
                let outputs = downstream.get(&current_id);
                if outputs.is_none() || outputs.unwrap().is_empty() {
                    break;
                }

                let outputs = outputs.unwrap();
                if outputs.len() != 1 {
                    // Multiple outputs - can't continue chain
                    break;
                }

                let (next_id, edge) = &outputs[0];

                // Check if next operator is already a chain head
                if chain_heads.contains(next_id) {
                    break;
                }

                let next_vertex = vertex_map.get(next_id).unwrap().clone();

                // Try to append to chain
                if !chain.try_append(next_vertex, edge) {
                    break;
                }

                current_id = next_id.clone();
            }

            // Record which chain each vertex belongs to
            let chain_idx = chains.len();
            for op_id in &chain.operator_ids {
                vertex_to_chain.insert(op_id.clone(), chain_idx);
            }

            chains.push(chain);
        }

        chains
    }

    /// Build edges between chains.
    fn build_chain_edges(&self, graph: &JobGraph, chains: &[OperatorChain]) -> Vec<ChainEdge> {
        // Map from vertex ID to chain ID
        let vertex_to_chain: HashMap<_, _> = chains.iter()
            .flat_map(|c| c.operator_ids.iter().map(|id| (id.clone(), c.id.clone())))
            .collect();

        let mut chain_edges = Vec::new();
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();

        for edge in &graph.edges {
            let source_chain = vertex_to_chain.get(&edge.source_id).unwrap();
            let target_chain = vertex_to_chain.get(&edge.target_id).unwrap();

            // Only add edge if it crosses chain boundaries
            if source_chain != target_chain {
                let edge_key = (source_chain.clone(), target_chain.clone());
                if !seen_edges.contains(&edge_key) {
                    seen_edges.insert(edge_key);
                    chain_edges.push(ChainEdge {
                        source_chain_id: source_chain.clone(),
                        target_chain_id: target_chain.clone(),
                        partition_strategy: edge.partition_strategy,
                    });
                }
            }
        }

        chain_edges
    }

    /// Group chains by slot sharing group.
    fn group_by_slot_sharing(&self, chains: &[OperatorChain]) -> HashMap<String, Vec<usize>> {
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();

        for (idx, chain) in chains.iter().enumerate() {
            groups
                .entry(chain.slot_sharing_group.clone())
                .or_default()
                .push(idx);
        }

        groups
    }

    /// Calculate minimum slots needed considering slot sharing.
    fn calculate_min_slots(&self, chains: &[OperatorChain], groups: &HashMap<String, Vec<usize>>) -> u32 {
        if !self.config.slot_sharing_enabled {
            // Without slot sharing, need sum of all parallelisms
            return chains.iter().map(|c| c.parallelism).sum();
        }

        // With slot sharing, need max parallelism within each group
        groups.values()
            .map(|chain_indices| {
                chain_indices.iter()
                    .map(|&idx| chains[idx].parallelism)
                    .max()
                    .unwrap_or(0)
            })
            .sum()
    }
}

impl Default for JobGraphOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizedJobGraph {
    /// Print optimization summary to stderr.
    pub fn print_summary(&self) {
        eprintln!("=== Job Graph Optimization Summary ===");
        eprintln!("Job: {}", self.name);
        eprintln!();
        eprintln!("Operators: {} -> {} chains", self.stats.operators_before, self.stats.chains_after);
        eprintln!("Tasks: {} -> {} ({:.1}% reduction)",
            self.stats.tasks_before,
            self.stats.tasks_after,
            self.stats.task_reduction_percent()
        );
        eprintln!("Slot sharing groups: {}", self.stats.slot_sharing_groups);
        eprintln!("Minimum slots needed: {}", self.stats.min_slots_needed);
        eprintln!();
        eprintln!("Chains:");
        for (i, chain) in self.chains.iter().enumerate() {
            eprintln!("  [{}] {} (p={})", i, chain.name, chain.parallelism);
        }
    }

    /// Format optimization summary as a string.
    pub fn summary_string(&self) -> String {
        let mut s = String::new();
        s.push_str("=== Job Graph Optimization Summary ===\n");
        s.push_str(&format!("Job: {}\n\n", self.name));
        s.push_str(&format!("Operators: {} -> {} chains\n", self.stats.operators_before, self.stats.chains_after));
        s.push_str(&format!("Tasks: {} -> {} ({:.1}% reduction)\n",
            self.stats.tasks_before,
            self.stats.tasks_after,
            self.stats.task_reduction_percent()
        ));
        s.push_str(&format!("Slot sharing groups: {}\n", self.stats.slot_sharing_groups));
        s.push_str(&format!("Minimum slots needed: {}\n\n", self.stats.min_slots_needed));
        s.push_str("Chains:\n");
        for (i, chain) in self.chains.iter().enumerate() {
            s.push_str(&format!("  [{}] {} (p={})\n", i, chain.name, chain.parallelism));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::*;

    #[test]
    fn test_simple_chaining() {
        let mut graph = JobGraph::new("test");

        // Source -> Map -> Filter -> Sink (all p=1, forward)
        graph.vertices = vec![
            Vertex::new("source", "Source", OperatorType::Source {
                connector: ConnectorConfig::socket("localhost", 9999),
            }),
            Vertex::new("map", "Map", OperatorType::Map),
            Vertex::new("filter", "Filter", OperatorType::Filter),
            Vertex::new("sink", "Sink", OperatorType::Sink {
                connector: ConnectorConfig::socket("localhost", 9998),
            }),
        ];

        graph.edges = vec![
            Edge::new("source", "map"),
            Edge::new("map", "filter"),
            Edge::new("filter", "sink"),
        ];

        let optimizer = JobGraphOptimizer::new();
        let optimized = optimizer.optimize(&graph);

        // Should produce 1 chain: Source -> Map -> Filter -> Sink
        assert_eq!(optimized.chains.len(), 1);
        assert_eq!(optimized.chains[0].operators.len(), 4);
        assert_eq!(optimized.stats.tasks_before, 4);
        assert_eq!(optimized.stats.tasks_after, 1);
    }

    #[test]
    fn test_parallelism_breaks_chain() {
        let mut graph = JobGraph::new("test");

        // Source(p=1) -> Map(p=2) - different parallelism breaks chain
        graph.vertices = vec![
            Vertex::new("source", "Source", OperatorType::Source {
                connector: ConnectorConfig::socket("localhost", 9999),
            }),
            Vertex::new("map", "Map", OperatorType::Map).with_parallelism(2),
            Vertex::new("sink", "Sink", OperatorType::Sink {
                connector: ConnectorConfig::socket("localhost", 9998),
            }).with_parallelism(2),
        ];

        graph.edges = vec![
            Edge::new("source", "map").with_strategy(PartitionStrategy::Rebalance),
            Edge::new("map", "sink"),
        ];

        let optimizer = JobGraphOptimizer::new();
        let optimized = optimizer.optimize(&graph);

        // Should produce 2 chains: [Source] and [Map -> Sink]
        assert_eq!(optimized.chains.len(), 2);
    }
}
