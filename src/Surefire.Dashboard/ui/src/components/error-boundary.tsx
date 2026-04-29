import React from "react";
import {useLocation} from "react-router";

type ErrorBoundaryProps = {
  children: React.ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
};

class ErrorBoundaryInner extends React.Component<
  ErrorBoundaryProps & { locationKey: string },
  ErrorBoundaryState
> {
  public state: ErrorBoundaryState = {hasError: false};

  public static getDerivedStateFromError(): ErrorBoundaryState {
    return {hasError: true};
  }

  public componentDidUpdate(
    prevProps: ErrorBoundaryProps & { locationKey: string },
  ): void {
    if (this.state.hasError && prevProps.locationKey !== this.props.locationKey) {
      this.setState({hasError: false});
    }
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

export function ErrorBoundary({children}: ErrorBoundaryProps) {
  const location = useLocation();
  return (
    <ErrorBoundaryInner locationKey={location.pathname}>
      {children}
    </ErrorBoundaryInner>
  );
}
