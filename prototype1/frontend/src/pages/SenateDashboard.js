/**
 * Senate Dashboard - Institution-wide Analytics
 * Read-only access to all analytics and reports
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  HStack,
  Text,
  Button,
  IconButton,
  SimpleGrid,
  Card,
  CardBody,
  Spinner,
} from '@chakra-ui/react';
import { FaDownload, FaFilePdf, FaFileExcel, FaShare } from 'react-icons/fa';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import StatsCards from '../components/StatsCards';
import Charts from '../components/Charts';
import axios from 'axios';

const SenateDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState(null);
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadDashboardData();
  }, [filters]);

  const loadDashboardData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/dashboard', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: filters
      });
      setStats(response.data);
    } catch (err) {
      console.error('Error loading dashboard:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleFilterChange = (newFilters) => {
    setFilters(newFilters);
  };

  const exportReport = async (format) => {
    try {
      const response = await axios.get(`/api/analytics/export/${format}`, {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: filters,
        responseType: 'blob'
      });
      // Handle file download
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `ucu_report_${new Date().toISOString()}.${format}`);
      document.body.appendChild(link);
      link.click();
    } catch (err) {
      console.error('Export error:', err);
    }
  };

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          {/* Header */}
          <HStack justify="space-between">
            <Box>
              <Heading size="xl" color="blue.600">Senate Dashboard</Heading>
              <Text color="gray.600">Institution-wide Analytics & Reports</Text>
            </Box>
            <HStack>
              <Button leftIcon={<FaFilePdf />} onClick={() => exportReport('pdf')} colorScheme="red">
                Export PDF
              </Button>
              <Button leftIcon={<FaFileExcel />} onClick={() => exportReport('xlsx')} colorScheme="green">
                Export Excel
              </Button>
              <Button leftIcon={<FaShare />} variant="outline">
                Share Report
              </Button>
            </HStack>
          </HStack>

          {/* Global Filter Panel */}
          <GlobalFilterPanel onFilterChange={handleFilterChange} />

          {loading ? (
            <Box textAlign="center" py={20}>
              <Spinner size="xl" color="blue.500" />
            </Box>
          ) : (
            <>
              <StatsCards stats={stats} />
              <Charts data={stats} filters={filters} type="senate" />
            </>
          )}
        </VStack>
      </Container>
    </Box>
  );
};

export default SenateDashboard;


