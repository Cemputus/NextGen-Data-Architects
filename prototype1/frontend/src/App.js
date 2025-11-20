/**
 * Main App Component with Role-Based Routing
 * Multi-page application with role-specific dashboards
 */
import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ChakraProvider, Spinner, Center, Box } from '@chakra-ui/react';
import { AuthProvider, useAuth } from './context/AuthContext';
import { rbac } from './utils/rbac';
import theme from './theme';

// Layout
import Layout from './components/Layout';

// Auth
import Login from './components/Login';

// Role-specific Dashboards
import StudentDashboard from './pages/StudentDashboard';
import StaffDashboard from './pages/StaffDashboard';
import HODDashboard from './pages/HODDashboard';
import DeanDashboard from './pages/DeanDashboard';
import SenateDashboard from './pages/SenateDashboard';
import AnalystDashboard from './pages/AnalystDashboard';
import AdminDashboard from './pages/AdminDashboard';
import HRDashboard from './pages/HRDashboard';
import FinanceDashboard from './pages/FinanceDashboard';

// Shared Pages
import FEXAnalytics from './pages/FEXAnalytics';
import HighSchoolAnalytics from './pages/HighSchoolAnalytics';
import ProfilePage from './pages/ProfilePage';
import PredictionPage from './pages/PredictionPage';

function PrivateRoute({ children, requiredRole = null }) {
  const { isAuthenticated, loading, user } = useAuth();
  
  if (loading) {
    return (
      <Box minH="100vh" bg="gray.50">
        <Center minH="100vh">
          <Spinner size="xl" color="blue.500" thickness="4px" />
        </Center>
      </Box>
    );
  }
  
  if (!isAuthenticated) {
    return <Navigate to="/login" />;
  }
  
  if (requiredRole && user?.role !== requiredRole) {
    return <Navigate to={rbac.getDefaultRoute(user?.role)} />;
  }
  
  return <Layout>{children}</Layout>;
}

function RoleRoute({ children, allowedRoles }) {
  const { user } = useAuth();
  
  if (!allowedRoles.includes(user?.role)) {
    return <Navigate to={rbac.getDefaultRoute(user?.role)} />;
  }
  
  return children;
}

function App() {
  return (
    <ChakraProvider theme={theme}>
      <AuthProvider>
        <Router>
          <Routes>
            {/* Public Routes */}
            <Route path="/login" element={<Login />} />
            
            {/* Student Routes */}
            <Route
              path="/student/*"
              element={
                <PrivateRoute requiredRole="student">
                  <Routes>
                    <Route path="dashboard" element={<StudentDashboard />} />
                    <Route path="grades" element={<StudentDashboard />} />
                    <Route path="attendance" element={<StudentDashboard />} />
                    <Route path="payments" element={<StudentDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="predictions" element={<PredictionPage />} />
                    <Route path="*" element={<Navigate to="/student/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Staff Routes */}
            <Route
              path="/staff/*"
              element={
                <PrivateRoute requiredRole="staff">
                  <Routes>
                    <Route path="dashboard" element={<StaffDashboard />} />
                    <Route path="classes" element={<StaffDashboard />} />
                    <Route path="analytics" element={<StaffDashboard />} />
                    <Route path="predictions" element={<PredictionPage />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/staff/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* HOD Routes */}
            <Route
              path="/hod/*"
              element={
                <PrivateRoute requiredRole="hod">
                  <Routes>
                    <Route path="dashboard" element={<HODDashboard />} />
                    <Route path="analytics" element={<HODDashboard />} />
                    <Route path="fex" element={<FEXAnalytics />} />
                    <Route path="high-school" element={<HighSchoolAnalytics />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/hod/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Dean Routes */}
            <Route
              path="/dean/*"
              element={
                <PrivateRoute requiredRole="dean">
                  <Routes>
                    <Route path="dashboard" element={<DeanDashboard />} />
                    <Route path="analytics" element={<DeanDashboard />} />
                    <Route path="fex" element={<FEXAnalytics />} />
                    <Route path="high-school" element={<HighSchoolAnalytics />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/dean/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Senate Routes */}
            <Route
              path="/senate/*"
              element={
                <PrivateRoute requiredRole="senate">
                  <Routes>
                    <Route path="dashboard" element={<SenateDashboard />} />
                    <Route path="analytics" element={<SenateDashboard />} />
                    <Route path="fex" element={<FEXAnalytics />} />
                    <Route path="high-school" element={<HighSchoolAnalytics />} />
                    <Route path="reports" element={<SenateDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/senate/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Analyst Routes */}
            <Route
              path="/analyst/*"
              element={
                <PrivateRoute requiredRole="analyst">
                  <Routes>
                    <Route path="dashboard" element={<AnalystDashboard />} />
                    <Route path="analytics" element={<AnalystDashboard />} />
                    <Route path="fex" element={<FEXAnalytics />} />
                    <Route path="high-school" element={<HighSchoolAnalytics />} />
                    <Route path="predictions" element={<PredictionPage />} />
                    <Route path="reports" element={<AnalystDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/analyst/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Admin Routes */}
            <Route
              path="/admin/*"
              element={
                <PrivateRoute requiredRole="sysadmin">
                  <Routes>
                    <Route path="dashboard" element={<AdminDashboard />} />
                    <Route path="users" element={<AdminDashboard />} />
                    <Route path="settings" element={<AdminDashboard />} />
                    <Route path="etl" element={<AdminDashboard />} />
                    <Route path="audit" element={<AdminDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/admin/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* HR Routes */}
            <Route
              path="/hr/*"
              element={
                <PrivateRoute requiredRole="hr">
                  <Routes>
                    <Route path="dashboard" element={<HRDashboard />} />
                    <Route path="analytics" element={<HRDashboard />} />
                    <Route path="staff" element={<HRDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/hr/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Finance Routes */}
            <Route
              path="/finance/*"
              element={
                <PrivateRoute requiredRole="finance">
                  <Routes>
                    <Route path="dashboard" element={<FinanceDashboard />} />
                    <Route path="analytics" element={<FinanceDashboard />} />
                    <Route path="payments" element={<FinanceDashboard />} />
                    <Route path="profile" element={<ProfilePage />} />
                    <Route path="*" element={<Navigate to="/finance/dashboard" />} />
                  </Routes>
                </PrivateRoute>
              }
            />
            
            {/* Default Route - Redirect based on role */}
            <Route
              path="/"
              element={
                <PrivateRoute>
                  <Navigate to="/dashboard" />
                </PrivateRoute>
              }
            />
            
            {/* Legacy dashboard route - redirect to role-specific */}
            <Route
              path="/dashboard"
              element={
                <PrivateRoute>
                  <RoleRedirect />
                </PrivateRoute>
              }
            />
          </Routes>
        </Router>
      </AuthProvider>
    </ChakraProvider>
  );
}

function RoleRedirect() {
  const { user } = useAuth();
  const defaultRoute = rbac.getDefaultRoute(user?.role);
  return <Navigate to={defaultRoute} />;
}

export default App;
