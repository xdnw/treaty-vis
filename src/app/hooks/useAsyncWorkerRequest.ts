import { useEffect, useRef, useState } from "react";

type UseAsyncWorkerRequestParams<TData> = {
  enabled?: boolean;
  dependencies: readonly unknown[];
  initialData: TData;
  keepDataOnRefresh?: boolean;
  resetOnDisable?: boolean;
  request: () => Promise<TData>;
  formatError?: (reason: unknown) => string;
  onError?: (message: string) => void;
};

export function useAsyncWorkerRequest<TData>(params: UseAsyncWorkerRequestParams<TData>): {
  data: TData;
  error: string | null;
} {
  const {
    enabled = true,
    dependencies,
    initialData,
    keepDataOnRefresh = true,
    resetOnDisable = true,
    request,
    formatError,
    onError
  } = params;

  const [data, setData] = useState<TData>(initialData);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (!enabled) {
      if (resetOnDisable) {
        setData(initialData);
        setError(null);
      }
      return;
    }

    requestIdRef.current += 1;
    const requestId = requestIdRef.current;
    if (!keepDataOnRefresh) {
      setData(initialData);
    }
    setError(null);

    void request()
      .then((nextData) => {
        if (requestIdRef.current !== requestId) {
          return;
        }
        setData(nextData);
      })
      .catch((reason) => {
        if (requestIdRef.current !== requestId) {
          return;
        }

        const message = formatError
          ? formatError(reason)
          : reason instanceof Error
            ? reason.message
            : "Unknown async request error";
        setError(message);
        onError?.(message);
      });
  }, dependencies);

  return { data, error };
}
