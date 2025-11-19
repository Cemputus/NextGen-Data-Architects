import React, { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';
import StatsCards from './StatsCards';
import Charts from './Charts';
import PredictionPanel from './PredictionPanel';
import './Dashboard.css';

const Dashboard = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [chartData, setChartData] = useState({});

  useEffect(() => {
    fetchDashboardData();
  }, []);

  const fetchDashboardData = async () => {
    try {
      const [statsRes, deptRes, gradesRes, paymentRes, attendanceRes, gradeDistRes, topStudentsRes] = 
        await Promise.all([
          axios.get('/api/dashboard/stats'),
          axios.get('/api/dashboard/students-by-department'),
          axios.get('/api/dashboard/grades-over-time'),
          axios.get('/api/dashboard/payment-status'),
          axios.get('/api/dashboard/attendance-by-course'),
          axios.get('/api/dashboard/grade-distribution'),
          axios.get('/api/dashboard/top-students')
        ]);

      setStats(statsRes.data);
      setChartData({
        departments: deptRes.data,
        gradesOverTime: gradesRes.data,
        paymentStatus: paymentRes.data,
        attendance: attendanceRes.data,
        gradeDistribution: gradeDistRes.data,
        topStudents: topStudentsRes.data
      });
      setLoading(false);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      setLoading(false);
    }
  };

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const handleDownloadPDF = async () => {
    try {
      const response = await axios.post('/api/report/generate', {}, {
        responseType: 'blob'
      });
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `university_report_${new Date().toISOString().split('T')[0]}.pdf`);
      document.body.appendChild(link);
      link.click();
      link.remove();
    } catch (error) {
      // If API endpoint doesn't support blob, generate client-side
      window.open('http://localhost:5000/api/report/generate', '_blank');
    }
  };

  if (loading) {
    return (
      <div className="dashboard-container">
        <div className="loading">Loading dashboard...</div>
      </div>
    );
  }

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <div className="header-content">
          <h1>UCU Analytics Dashboard</h1>
          <div className="header-actions">
            <span className="user-info">Welcome, {user?.username}</span>
            <button onClick={handleDownloadPDF} className="download-btn">
              Download Report
            </button>
            <button onClick={handleLogout} className="logout-btn">
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="dashboard-main">
        <StatsCards stats={stats} />
        <Charts chartData={chartData} />
        <PredictionPanel />
      </main>
    </div>
  );
};

export default Dashboard;




