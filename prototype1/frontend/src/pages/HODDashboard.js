/**
 * HOD Dashboard - Department Analytics
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  Text,
  SimpleGrid,
  Card,
  CardBody,
  Spinner,
} from '@chakra-ui/react';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import StatsCards from '../components/StatsCards';
import Charts from '../components/Charts';
import axios from 'axios';

const HODDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadDepartmentData();
  }, [filters]);

  const loadDepartmentData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/department', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: filters
      });
      setStats(response.data);
    } catch (err) {
      console.error('Error loading department data:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <Heading size="xl" color="blue.600">Department Dashboard</Heading>
          <Text color="gray.600">Department-wide Analytics & Insights</Text>
          
          <GlobalFilterPanel onFilterChange={setFilters} />
          
          {loading ? (
            <Spinner size="xl" />
          ) : (
            <>
              <StatsCards stats={stats} />
              <Charts data={stats} filters={filters} type="department" />
            </>
          )}
        </VStack>
      </Container>
    </Box>
  );
};

export default HODDashboard;


