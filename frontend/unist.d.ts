declare module 'unist' {
  export interface Point {
    offset?: number | null;
    line?: number | null;
    column?: number | null;
  }

  export interface Position {
    start?: Point;
    end?: Point;
  }

  export interface Node {
    type: string;
    data?: Record<string, unknown>;
    position?: Position;
  }

  export interface Parent extends Node {
    children: Node[];
  }
}
