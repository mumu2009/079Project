import { render, screen } from '@testing-library/react';
import App from './App';

test('renders shell', () => {
  render(<App />);
  expect(screen.getByText(/079 phoenix/i)).toBeInTheDocument();
  expect(screen.getByText('新会话')).toBeInTheDocument();
});
