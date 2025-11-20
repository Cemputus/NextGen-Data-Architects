/**
 * FEX Analytics Page
 * Comprehensive FEX analysis with drilldown capabilities
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  HStack,
  Text,
  Select,
  Button,
  SimpleGrid,
  Card,
  CardBody,
  Spinner,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react';
import { FaChartBar, FaTable, FaDownload } from 'react-icons/fa';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import axios from 'axios';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell } from 'recharts';

const FEXAnalytics = () => {
  const [loading, setLoading] = useState(true);
  const [fexData, setFexData] = useState(null);
  const [drilldown, setDrilldown] = useState('overall');
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadFEXData();
  }, [filters, drilldown]);

  const loadFEXData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/fex', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: { ...filters, drilldown }
      });
      setFexData(response.data);
    } catch (err) {
      console.error('Error loading FEX data:', err);
    } finally {
      setLoading(false);
    }
  };

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d'];

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          {/* Header */}
          <HStack justify="space-between">
            <Box>
              <Heading size="xl" color="blue.600">FEX Analytics</Heading>
              <Text color="gray.600">Failed Exam Analysis with Drilldown</Text>
            </Box>
            <HStack>
              <Select
                value={drilldown}
                onChange={(e) => setDrilldown(e.target.value)}
                maxW="200px"
              >
                <option value="overall">Overall</option>
                <option value="faculty">By Faculty</option>
                <option value="department">By Department</option>
                <option value="program">By Program</option>
                <option value="course">By Course</option>
              </Select>
              <Button leftIcon={<FaDownload />} variant="outline">
                Export
              </Button>
            </HStack>
          </HStack>

          {/* Global Filter Panel */}
          <GlobalFilterPanel onFilterChange={setFilters} />

          {loading ? (
            <Box textAlign="center" py={20}>
              <Spinner size="xl" color="blue.500" />
            </Box>
          ) : fexData && (
            <>
              {/* Summary Cards */}
              <SimpleGrid columns={{ base: 2, md: 4 }} spacing={4}>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Total FEX</Text>
                    <Heading size="lg" color="red.500">{fexData.summary?.total_fex || 0}</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">FEX Rate</Text>
                    <Heading size="lg" color="orange.500">{fexData.summary?.fex_rate || 0}%</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Total MEX</Text>
                    <Heading size="lg" color="yellow.500">{fexData.summary?.total_mex || 0}</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Total FCW</Text>
                    <Heading size="lg" color="purple.500">{fexData.summary?.total_fcw || 0}</Heading>
                  </CardBody>
                </Card>
              </SimpleGrid>

              {/* Charts */}
              <Tabs colorScheme="blue">
                <TabList>
                  <Tab>FEX Distribution</Tab>
                  <Tab>Trend Analysis</Tab>
                  <Tab>Comparison</Tab>
                  <Tab>Detailed Table</Tab>
                </TabList>

                <TabPanels>
                  <TabPanel>
                    <Card>
                      <CardBody>
                        <ResponsiveContainer width="100%" height={400}>
                          <BarChart data={fexData.data}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey={drilldown === 'faculty' ? 'faculty_name' : drilldown === 'department' ? 'department' : 'program_name'} />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Bar dataKey="total_fex" fill="#FF8042" name="FEX" />
                            <Bar dataKey="total_mex" fill="#FFBB28" name="MEX" />
                            <Bar dataKey="total_fcw" fill="#8884d8" name="FCW" />
                          </BarChart>
                        </ResponsiveContainer>
                      </CardBody>
                    </Card>
                  </TabPanel>
                  <TabPanel>
                    <Text>Trend analysis charts</Text>
                  </TabPanel>
                  <TabPanel>
                    <Text>Comparison charts</Text>
                  </TabPanel>
                  <TabPanel>
                    <Text>Detailed data table</Text>
                  </TabPanel>
                </TabPanels>
              </Tabs>
            </>
          )}
        </VStack>
      </Container>
    </Box>
  );
};

export default FEXAnalytics;


