import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

type Report = {
  client_id: string;
  ds: string;
  segment?: string | null;
  country?: string | null;
  plan?: string | null;
  events_cnt: number;
  errors_cnt: number;
  avg_latency_ms: number;
  p95_latency_ms: number;
  sessions_cnt: number;
  last_event_at?: string | null;
  loaded_at: string;
};

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<Report | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with status ${response.status}`);
      }
      
      const data: Report = await response.json();
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setReport(null);
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>
        
        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {report && (
          <div className="mt-6 text-sm">
            <h2 className="text-xl font-semibold mb-2">Your report</h2>
            <div className="grid grid-cols-2 gap-2">
              <div><strong>Client:</strong> {report.client_id}</div>
              <div><strong>Date:</strong> {report.ds}</div>
              <div><strong>Segment:</strong> {report.segment ?? '—'}</div>
              <div><strong>Country:</strong> {report.country ?? '—'}</div>
              <div><strong>Plan:</strong> {report.plan ?? '—'}</div>
              <div><strong>Events:</strong> {report.events_cnt}</div>
              <div><strong>Errors:</strong> {report.errors_cnt}</div>
              <div><strong>Avg latency (ms):</strong> {report.avg_latency_ms}</div>
              <div><strong>P95 latency (ms):</strong> {report.p95_latency_ms}</div>
              <div><strong>Sessions:</strong> {report.sessions_cnt}</div>
              <div><strong>Last event at:</strong> {report.last_event_at ?? '—'}</div>
              <div><strong>Loaded at:</strong> {report.loaded_at}</div>
            </div>
          </div>
        )}

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;