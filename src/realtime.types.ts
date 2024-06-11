import type { Operation } from "fast-json-patch";
export type { Operation };

export interface Env {
  REALTIME: DurableObjectNamespace;
  EPHEMERAL_REALTIME: DurableObjectNamespace;
  WORKER_PUBLIC_KEY: string;
  WORKER_PRIVATE_KEY: string;
}

export interface BaseFilePatch {
  path: string;
}
export type TextFilePatchOperation = InsertAtOperation | DeleteAtOperation;

export interface TextFielPatchOperationBase {
  at: number;
}

export interface InsertAtOperation extends TextFielPatchOperationBase {
  text: string;
}

export interface DeleteAtOperation extends TextFielPatchOperationBase {
  length: number;
}
export interface TextFilePatch extends BaseFilePatch {
  operations: TextFilePatchOperation[];
  timestamp: number;
}

export interface TextFileSet extends BaseFilePatch {
  content: string | null;
}

export type FilePatch = JSONFilePatch | TextFilePatch | TextFileSet;

export const isJSONFilePatch = (patch: FilePatch): patch is JSONFilePatch => {
  return (patch as JSONFilePatch).patches !== undefined;
};

export const isTextFileSet = (patch: FilePatch): patch is TextFileSet => {
  return (patch as TextFileSet).content !== undefined;
};

export const isTextFilePatch = (patch: FilePatch): patch is TextFilePatch => {
  return (patch as TextFilePatch).timestamp !== undefined && (patch as TextFilePatch).operations?.length >= 0;
}

export interface JSONFilePatch extends BaseFilePatch {
  patches: Operation[];
}

export interface VolumePatchRequest {
  messageId?: string;
  patches: FilePatch[];
}

export interface FilePatchResult {
  path: string;
  accepted: boolean;
  content?: string | null;
  deleted?: boolean;
}

export interface VolumePatchResponse {
  results: FilePatchResult[];
  timestamp: number;
}

export interface FsEvent {
  messageId?: string;
  path: string;
  deleted?: boolean;
  timestamp: number;
}

export type ServerEvent = FsEvent;

export interface File {
  content: string;
}

export type FileSystemNode = Record<string, File>;
