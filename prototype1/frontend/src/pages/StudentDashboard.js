/**
 * Student Dashboard - Personal Analytics
 * Students can only view their own data
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  HStack,
  Heading,
  Text,
  Card,
  CardBody,
  SimpleGrid,
  Spinner,
  Alert,
  AlertIcon,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react';
import { FaGraduationCap, FaChartLine, FaBook, FaMoneyBillWave } from 'react-icons/fa';
import axios from 'axios';
import StatsCards from '../components/StatsCards';
import Charts from '../components/Charts';
import { useAuth } from '../context/AuthContext';

const StudentDashboard = () => {
  const { user } = useAuth();
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadStudentData();
  }, []);

  const loadStudentData = async () => {
    try {
      setLoading(true);
      // Load student-specific data
      const response = await axios.get('/api/analytics/student', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: {
          access_number: user?.access_number || user?.username
        }
      });
      setStats(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <Box minH="100vh" bg="gray.50" display="flex" alignItems="center" justifyContent="center">
        <Spinner size="xl" color="blue.500" thickness="4px" />
      </Box>
    );
  }

  if (error) {
    return (
      <Container maxW="container.xl" py={8}>
        <Alert status="error">
          <AlertIcon />
          {error}
        </Alert>
      </Container>
    );
  }

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          {/* Header */}
          <Box>
            <HStack spacing={3} mb={2}>
              <FaGraduationCap size={32} color="#3182CE" />
              <Heading size="xl" color="blue.600">
                My Dashboard
              </Heading>
            </HStack>
            <Text color="gray.600">
              Welcome, {user?.first_name} {user?.last_name} ({user?.access_number})
            </Text>
          </Box>

          {/* Stats Cards */}
          <StatsCards stats={stats} />

          {/* Analytics Tabs */}
          <Tabs colorScheme="blue" variant="enclosed">
            <TabList>
              <Tab>Academic Performance</Tab>
              <Tab>Attendance</Tab>
              <Tab>Payments</Tab>
              <Tab>Course Progress</Tab>
            </TabList>

            <TabPanels>
              <TabPanel>
                <Charts data={stats} type="student" />
              </TabPanel>
              <TabPanel>
                <Text>Attendance charts and data</Text>
              </TabPanel>
              <TabPanel>
                <Text>Payment history and status</Text>
              </TabPanel>
              <TabPanel>
                <Text>Course enrollment and progress</Text>
              </TabPanel>
            </TabPanels>
          </Tabs>
        </VStack>
      </Container>
    </Box>
  );
};

export default StudentDashboard;


