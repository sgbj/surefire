import React from "react";

type ErrorBoundaryProps = {
  children: React.ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
};

export class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  public state: ErrorBoundaryState = { hasError: false };

  public static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  public componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error("Dashboard render error", error, errorInfo);
  }

  private handleReload = (): void => {
    window.location.reload();
  };

  public render(): React.ReactNode {
    if (this.state.hasError) {
      return (
        <div className="mx-auto mt-12 max-w-xl space-y-4 rounded-md border p-6">
          <h2 className="text-xl font-semibold tracking-tight">
            Something went wrong
          </h2>
          <p className="text-sm text-muted-foreground">
            The dashboard hit an unexpected error while rendering this page.
          </p>
          <button
            type="button"
            onClick={this.handleReload}
            className="inline-flex items-center rounded-md border px-3 py-2 text-sm font-medium hover:bg-muted"
          >
            Reload dashboard
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}
