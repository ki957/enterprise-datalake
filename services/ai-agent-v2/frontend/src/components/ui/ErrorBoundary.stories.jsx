import { ErrorBoundary } from './ErrorBoundary';

const meta = {
  title: 'UI/ErrorBoundary',
  component: ErrorBoundary,
  parameters: { layout: 'fullscreen' },
};

export default meta;

function ThrowError() {
  throw new Error('Simulated component error');
}

export const Default = {
  render: () => (
    <ErrorBoundary>
      <div>Normal content — no error thrown.</div>
    </ErrorBoundary>
  ),
};

export const WithError = {
  render: () => (
    <ErrorBoundary>
      <ThrowError />
    </ErrorBoundary>
  ),
};
