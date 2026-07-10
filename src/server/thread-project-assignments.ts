type LocalThreadProjectAssignment = {
  projectId: string;
  projectKind: "local";
  path?: string;
  cwd?: string;
  pendingCoreUpdate: boolean;
};

type RemoteThreadProjectAssignment = {
  projectId: string;
  projectKind: "remote";
  path: string;
  cwd?: string;
  hostId?: string;
  pendingCoreUpdate: boolean;
};

export type ThreadProjectAssignment =
  | LocalThreadProjectAssignment
  | RemoteThreadProjectAssignment;

export type ThreadProjectAssignments = Record<
  string,
  ThreadProjectAssignment
>;

export type SetAssignmentRequest = {
  threadId: string;
  assignment: ThreadProjectAssignment | null;
};

type ThreadProjectAssignmentsServiceOptions = {
  getValue: () => Promise<ThreadProjectAssignments>;
  notifyChanged: (request: SetAssignmentRequest) => void;
  setValue: (value: ThreadProjectAssignments) => Promise<void>;
};

export type ThreadProjectAssignmentsService = {
  setAssignment: (request: SetAssignmentRequest) => Promise<void>;
};

function assignmentValue(value: unknown): ThreadProjectAssignment | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const assignment = value as Record<string, unknown>;
  if (
    (assignment.projectKind !== "local" && assignment.projectKind !== "remote") ||
    typeof assignment.projectId !== "string" ||
    typeof assignment.pendingCoreUpdate !== "boolean"
  ) {
    return null;
  }
  if (
    (assignment.path !== undefined && typeof assignment.path !== "string") ||
    (assignment.cwd !== undefined && typeof assignment.cwd !== "string") ||
    (assignment.projectKind === "remote" &&
      (typeof assignment.path !== "string" ||
        (assignment.hostId !== undefined &&
          typeof assignment.hostId !== "string")))
  ) {
    return null;
  }
  if (assignment.projectKind === "local") {
    return {
      projectKind: "local",
      projectId: assignment.projectId,
      ...(assignment.path === undefined ? {} : { path: assignment.path }),
      ...(assignment.cwd === undefined ? {} : { cwd: assignment.cwd }),
      pendingCoreUpdate: assignment.pendingCoreUpdate,
    };
  }
  return {
    projectKind: "remote",
    projectId: assignment.projectId,
    path: assignment.path as string,
    ...(assignment.cwd === undefined ? {} : { cwd: assignment.cwd }),
    ...(assignment.hostId === undefined
      ? {}
      : { hostId: assignment.hostId as string }),
    pendingCoreUpdate: assignment.pendingCoreUpdate,
  };
}

function parseRequest(request: SetAssignmentRequest): SetAssignmentRequest {
  if (
    typeof request !== "object" ||
    request === null ||
    typeof request.threadId !== "string" ||
    request.threadId.length === 0
  ) {
    throw new Error("Invalid thread project assignment request");
  }
  const assignment =
    request.assignment === null ? null : assignmentValue(request.assignment);
  if (assignment === null && request.assignment !== null) {
    throw new Error("Invalid thread project assignment request");
  }
  return { threadId: request.threadId, assignment };
}

function assignmentPath(assignment: ThreadProjectAssignment): string | null {
  if (assignment.projectKind === "local") {
    return assignment.cwd ?? assignment.path ?? null;
  }
  return assignment.cwd ?? assignment.path;
}

function assignmentsEqual(
  left: ThreadProjectAssignment | undefined,
  right: ThreadProjectAssignment | null,
): boolean {
  if (left === undefined || right === null) {
    return left === undefined && right === null;
  }
  return (
    left.projectKind === right.projectKind &&
    left.projectId === right.projectId &&
    assignmentPath(left) === assignmentPath(right) &&
    left.pendingCoreUpdate === right.pendingCoreUpdate &&
    (left.projectKind === "remote" ? left.hostId ?? null : null) ===
      (right.projectKind === "remote" ? right.hostId ?? null : null)
  );
}

export function threadProjectAssignmentsValue(
  rawValue: unknown,
): ThreadProjectAssignments {
  if (
    typeof rawValue !== "object" ||
    rawValue === null ||
    Array.isArray(rawValue)
  ) {
    return {};
  }
  const assignments: ThreadProjectAssignments = {};
  for (const [threadId, assignmentValueRaw] of Object.entries(rawValue)) {
    const assignment = assignmentValue(assignmentValueRaw);
    if (assignment !== null) {
      assignments[threadId] = assignment;
    }
  }
  return assignments;
}

export function createThreadProjectAssignmentsService({
  getValue,
  notifyChanged,
  setValue,
}: ThreadProjectAssignmentsServiceOptions): ThreadProjectAssignmentsService {
  let pendingMutation = Promise.resolve();

  return {
    setAssignment(request): Promise<void> {
      const parsedRequest = parseRequest(request);
      const mutation = pendingMutation.then(async () => {
        const assignments = await getValue();
        if (
          assignmentsEqual(
            assignments[parsedRequest.threadId],
            parsedRequest.assignment,
          )
        ) {
          return;
        }
        const nextAssignments = { ...assignments };
        if (parsedRequest.assignment === null) {
          delete nextAssignments[parsedRequest.threadId];
        } else {
          nextAssignments[parsedRequest.threadId] = parsedRequest.assignment;
        }
        await setValue(nextAssignments);
        notifyChanged(parsedRequest);
      });
      pendingMutation = mutation.then(
        () => undefined,
        () => undefined,
      );
      return mutation;
    },
  };
}
