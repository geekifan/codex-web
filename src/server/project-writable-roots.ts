import fs from "node:fs/promises";
import path from "node:path";

export type WritableRoot = {
  kind: "local";
  path: string;
};

export type ProjectWritableRoots = Record<string, WritableRoot[]>;

export type AddRootRequest = {
  legacyRoot: string | null;
  projectId: string;
  root?: string;
};

export type RemoveRootRequest = {
  legacyRoot: string | null;
  projectId: string;
  root: string;
};

export type ClearRootsRequest = {
  legacyRoot: string | null;
  projectId: string;
};

export type ProjectWritableRootsResult = {
  changed: boolean;
  projectId: string;
  roots: WritableRoot[];
};

type ProjectWritableRootsServiceOptions = {
  getValue: () => Promise<ProjectWritableRoots>;
  getRootCompareKeys?: (root: string) => Promise<string[]>;
  getRootForStorage?: (root: string) => string;
  notifyChanged: () => void;
  selectWritableRoot?: (root: string | undefined) => Promise<string | null>;
  setValue: (value: ProjectWritableRoots) => Promise<void>;
};

export type ProjectWritableRootsService = {
  addRoot: (request: AddRootRequest) => Promise<ProjectWritableRootsResult>;
  clearRoots: (
    request: ClearRootsRequest,
  ) => Promise<ProjectWritableRootsResult>;
  removeRoot: (
    request: RemoveRootRequest,
  ) => Promise<ProjectWritableRootsResult>;
};

function validateProjectId(projectId: unknown): asserts projectId is string {
  if (typeof projectId !== "string" || projectId.trim().length === 0) {
    throw new Error("Invalid project writable roots request");
  }
}

function validateLegacyRoot(
  legacyRoot: unknown,
): asserts legacyRoot is string | null {
  if (
    legacyRoot !== null &&
    (typeof legacyRoot !== "string" || legacyRoot.length === 0)
  ) {
    throw new Error("Invalid project writable roots request");
  }
}

function validateRoot(root: unknown): asserts root is string {
  if (typeof root !== "string" || root.length === 0) {
    throw new Error("Invalid project writable roots request");
  }
}

function validateAddRootRequest(request: AddRootRequest): void {
  validateProjectId(request.projectId);
  validateLegacyRoot(request.legacyRoot);
  if (request.root !== undefined) {
    validateRoot(request.root);
  }
}

function validateRemoveRootRequest(request: RemoveRootRequest): void {
  validateProjectId(request.projectId);
  validateLegacyRoot(request.legacyRoot);
  validateRoot(request.root);
}

function validateClearRootsRequest(request: ClearRootsRequest): void {
  validateProjectId(request.projectId);
  validateLegacyRoot(request.legacyRoot);
}

function rootsForProject(
  value: ProjectWritableRoots,
  projectId: string,
  legacyRoot: string | null,
): WritableRoot[] {
  if (Object.hasOwn(value, projectId)) {
    return value[projectId] ?? [];
  }
  return legacyRoot === null ? [] : [{ kind: "local", path: legacyRoot }];
}

function comparisonKeysOverlap(
  left: readonly string[],
  right: readonly string[],
): boolean {
  return left.some((key) => right.includes(key));
}

export async function getRootCompareKeys(root: string): Promise<string[]> {
  const resolved = path.resolve(root);
  try {
    const stat = await fs.stat(resolved);
    if (!stat.isDirectory()) {
      return [path.normalize(resolved)];
    }
    return [
      path.normalize(resolved),
      path.normalize(await fs.realpath(resolved)),
    ];
  } catch {
    return [path.normalize(resolved)];
  }
}

export function createProjectWritableRootsService({
  getValue,
  getRootCompareKeys: getCompareKeys = getRootCompareKeys,
  getRootForStorage = (root) => root,
  notifyChanged,
  selectWritableRoot = async (root) => root ?? null,
  setValue,
}: ProjectWritableRootsServiceOptions): ProjectWritableRootsService {
  let pendingMutation = Promise.resolve();

  function runSerialized<T>(mutation: () => Promise<T>): Promise<T> {
    const result = pendingMutation.then(mutation, mutation);
    pendingMutation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  async function update(
    value: ProjectWritableRoots,
    projectId: string,
    roots: WritableRoot[],
  ): Promise<ProjectWritableRootsResult> {
    await setValue({ ...value, [projectId]: roots });
    notifyChanged();
    return { changed: true, projectId, roots };
  }

  return {
    async addRoot(request): Promise<ProjectWritableRootsResult> {
      validateAddRootRequest(request);
      const root = await selectWritableRoot(request.root);
      if (root === null) {
        const value = await getValue();
        return {
          changed: false,
          projectId: request.projectId,
          roots: rootsForProject(value, request.projectId, request.legacyRoot),
        };
      }

      return runSerialized(async () => {
        const value = await getValue();
        const roots = rootsForProject(
          value,
          request.projectId,
          request.legacyRoot,
        );
        const compareKeys = await getCompareKeys(root);
        const existingKeys = await Promise.all(
          roots.map((existingRoot) => getCompareKeys(existingRoot.path)),
        );
        if (
          existingKeys.some((keys) => comparisonKeysOverlap(keys, compareKeys))
        ) {
          return { changed: false, projectId: request.projectId, roots };
        }
        return update(value, request.projectId, [
          ...roots,
          { kind: "local", path: getRootForStorage(root) },
        ]);
      });
    },

    async removeRoot(request): Promise<ProjectWritableRootsResult> {
      validateRemoveRootRequest(request);
      return runSerialized(async () => {
        const value = await getValue();
        const roots = rootsForProject(
          value,
          request.projectId,
          request.legacyRoot,
        );
        if (roots.length === 0) {
          return { changed: false, projectId: request.projectId, roots: [] };
        }
        const compareKeys = await getCompareKeys(request.root);
        const keptRoots = (
          await Promise.all(
            roots.map(async (root) => ({
              compareKeys: await getCompareKeys(root.path),
              root,
            })),
          )
        )
          .filter(
            ({ compareKeys: existingKeys }) =>
              !comparisonKeysOverlap(existingKeys, compareKeys),
          )
          .map(({ root }) => root);
        if (keptRoots.length === roots.length) {
          return { changed: false, projectId: request.projectId, roots };
        }
        return update(value, request.projectId, keptRoots);
      });
    },

    async clearRoots(request): Promise<ProjectWritableRootsResult> {
      validateClearRootsRequest(request);
      return runSerialized(async () => {
        const value = await getValue();
        const hasStoredRoots = Object.hasOwn(value, request.projectId);
        const roots = rootsForProject(
          value,
          request.projectId,
          request.legacyRoot,
        );
        if (hasStoredRoots && roots.length === 0) {
          return { changed: false, projectId: request.projectId, roots: [] };
        }
        return update(value, request.projectId, []);
      });
    },
  };
}
