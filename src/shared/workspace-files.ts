export const WORKSPACE_FILE_CHUNK_BYTES = 256 * 1024;
export const WORKSPACE_FILE_MAX_BYTES = 256 * 1024 * 1024;

export type WorkspaceFileRepresentation = "auto" | "blob" | "text";

export type WorkspaceFileRendererMessage =
  | {
      type: "workspace-file-read-request";
      requestId: string;
      hostId: string;
      path: string;
      representation: WorkspaceFileRepresentation;
    }
  | {
      type: "workspace-file-write-start";
      requestId: string;
      transferId: string;
      hostId: string;
      path: string;
      ifMatch: string;
      byteLength: number;
    }
  | {
      type: "workspace-file-write-chunk";
      requestId: string;
      transferId: string;
      offset: number;
      bytes: Uint8Array;
    }
  | {
      type: "workspace-file-write-commit";
      requestId: string;
      transferId: string;
    }
  | {
      type: "workspace-file-write-abort";
      transferId: string;
    };

export type WorkspaceFileMainMessage =
  | {
      type: "workspace-file-read-start";
      requestId: string;
      transferId: string;
      etag: string;
      byteLength: number;
      representation: "blob" | "text";
    }
  | {
      type: "workspace-file-read-chunk";
      requestId: string;
      transferId: string;
      offset: number;
      bytes: Uint8Array;
    }
  | {
      type: "workspace-file-read-end";
      requestId: string;
      transferId: string;
    }
  | {
      type: "workspace-file-read-error";
      requestId: string;
      transferId?: string;
      errorMessage: string;
    }
  | {
      type: "workspace-file-write-ready";
      requestId: string;
      transferId: string;
    }
  | {
      type: "workspace-file-write-chunk-result";
      requestId: string;
      transferId: string;
      offset: number;
      ok: true;
    }
  | {
      type: "workspace-file-write-chunk-result";
      requestId: string;
      transferId: string;
      offset: number;
      ok: false;
      errorMessage: string;
    }
  | {
      type: "workspace-file-write-result";
      requestId: string;
      outcome: "saved";
      etag: string;
    }
  | {
      type: "workspace-file-write-result";
      requestId: string;
      outcome: "conflict";
      etag: string;
    }
  | {
      type: "workspace-file-write-result";
      requestId: string;
      outcome: "too-large";
      maxBytes: number;
    }
  | {
      type: "workspace-file-write-error";
      requestId: string;
      errorMessage: string;
    };

export function isWorkspaceFileRendererMessage(message: {
  type: string;
}): message is WorkspaceFileRendererMessage {
  return message.type.startsWith("workspace-file-");
}

export function isWorkspaceFileMainMessage(message: {
  type: string;
}): message is WorkspaceFileMainMessage {
  return message.type.startsWith("workspace-file-");
}
