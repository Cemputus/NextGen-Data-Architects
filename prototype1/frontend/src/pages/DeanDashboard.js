/**
 * Dean Dashboard - Faculty Analytics
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  Text,
  Spinner,
} from '@chakra-ui/react';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import StatsCards from '../components/StatsCards';
import Charts from '../components/Charts';
import axios from 'axios';

const DeanDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadFacultyData();
  }, [filters]);

  const loadFacultyData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/faculty', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: filters
      });
      setStats(response.data);
    } catch (err) {
      console.error('Error loading faculty data:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <Heading size="xl" color="blue.600">Faculty Dashboard</Heading>
          <Text color="gray.600">Faculty-wide Analytics & Insights</Text>
          
          <GlobalFilterPanel onFilterChange={setFilters} />
          
          {loading ? (
            <Spinner size="xl" />
          ) : (
            <>
              <StatsCards stats={stats} />
              <Charts data={stats} filters={filters} type="faculty" />
            </>
          )}
        </VStack>
      </Container>
    </Box>
  );
};

export default DeanDashboard;


