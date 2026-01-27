'use client'

import { useMemo } from 'react'
import {
  ReactFlow,
  Controls,
  MiniMap,
  Background,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  type Edge,
  type NodeTypes,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from 'dagre'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { OperatorNode } from './operator-node'
import { useJobGraph } from '@/lib/hooks/useApi'
import { getOperatorTypeColor } from '@/lib/utils'
import type { JobGraph } from '@/types'

const nodeTypes: NodeTypes = {
  operator: OperatorNode as any,
}

const NODE_WIDTH = 200
const NODE_HEIGHT = 100

function getLayoutedElements(graph: JobGraph) {
  const dagreGraph = new dagre.graphlib.Graph()
  dagreGraph.setDefaultEdgeLabel(() => ({}))
  dagreGraph.setGraph({ rankdir: 'TB', nodesep: 50, ranksep: 80 })

  // Add nodes
  graph.vertices.forEach((vertex) => {
    dagreGraph.setNode(vertex.id, { width: NODE_WIDTH, height: NODE_HEIGHT })
  })

  // Add edges
  graph.edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source_id, edge.target_id)
  })

  dagre.layout(dagreGraph)

  const nodes = graph.vertices.map((vertex) => {
    const nodeWithPosition = dagreGraph.node(vertex.id)
    return {
      id: vertex.id,
      type: 'operator',
      position: {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      },
      data: {
        ...vertex,
        label: vertex.name,
      },
    }
  })

  const edges: Edge[] = graph.edges.map((edge, index) => {
    const isBroadcast = edge.partition_strategy === 'Broadcast'
    return {
      id: `e${index}`,
      source: edge.source_id,
      target: edge.target_id,
      label: edge.partition_strategy,
      labelStyle: { fontSize: 10 },
      labelBgStyle: { fill: 'hsl(var(--background))' },
      style: {
        strokeWidth: isBroadcast ? 2 : 1,
        strokeDasharray: isBroadcast ? '5,5' : undefined,
      },
      animated: isBroadcast,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 15,
        height: 15,
      },
    }
  })

  return { nodes, edges }
}

interface JobGraphViewProps {
  jobId: string
}

export function JobGraphView({ jobId }: JobGraphViewProps) {
  const { data: graph, isLoading, error } = useJobGraph(jobId)

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(() => {
    if (!graph) return { nodes: [], edges: [] }
    return getLayoutedElements(graph)
  }, [graph])

  const [nodes, setNodes, onNodesChange] = useNodesState(layoutedNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedEdges)

  // Update nodes/edges when layout changes
  useMemo(() => {
    if (layoutedNodes.length > 0) {
      setNodes(layoutedNodes)
      setEdges(layoutedEdges)
    }
  }, [layoutedNodes, layoutedEdges, setNodes, setEdges])

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Job Graph</CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-[500px] w-full" />
        </CardContent>
      </Card>
    )
  }

  if (error || !graph) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Job Graph</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-[500px] flex items-center justify-center text-muted-foreground">
            {error ? 'Failed to load job graph' : 'No graph data available'}
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Job Graph</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="h-[500px] border rounded-lg overflow-hidden">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.1}
            maxZoom={2}
          >
            <Controls />
            <MiniMap
              nodeColor={(node) => {
                const operatorType = (node.data as any)?.operator_type ?? 'Unknown'
                return getOperatorTypeColor(operatorType)
              }}
              maskColor="hsl(var(--background) / 0.8)"
            />
            <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
          </ReactFlow>
        </div>
        <div className="mt-4 flex gap-4 flex-wrap">
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-green-500" />
            <span>Source</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-blue-500" />
            <span>Map/Filter</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-purple-500" />
            <span>KeyBy/Window</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <div className="w-3 h-3 rounded-full bg-red-500" />
            <span>Sink</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
