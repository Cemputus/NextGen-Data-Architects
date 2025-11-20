/**
 * High School Analytics Page
 * Analysis of high school performance, enrollment, retention, graduation rates
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  HStack,
  Text,
  SimpleGrid,
  Card,
  CardBody,
  Spinner,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Button,
} from '@chakra-ui/react';
import { FaDownload } from 'react-icons/fa';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import axios from 'axios';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from 'recharts';

const HighSchoolAnalytics = () => {
  const [loading, setLoading] = useState(true);
  const [hsData, setHsData] = useState(null);
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadHighSchoolData();
  }, [filters]);

  const loadHighSchoolData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/high-school', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` },
        params: filters
      });
      setHsData(response.data);
    } catch (err) {
      console.error('Error loading high school data:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <HStack justify="space-between">
            <Box>
              <Heading size="xl" color="blue.600">High School Analytics</Heading>
              <Text color="gray.600">Enrollment, Retention, Graduation, and Performance Analysis</Text>
            </Box>
            <Button leftIcon={<FaDownload />} variant="outline">
              Export
            </Button>
          </HStack>

          <GlobalFilterPanel onFilterChange={setFilters} />

          {loading ? (
            <Box textAlign="center" py={20}>
              <Spinner size="xl" color="blue.500" />
            </Box>
          ) : hsData && (
            <>
              {/* Summary Cards */}
              <SimpleGrid columns={{ base: 2, md: 4 }} spacing={4}>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Total High Schools</Text>
                    <Heading size="lg">{hsData.summary?.total_high_schools || 0}</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Total Students</Text>
                    <Heading size="lg">{hsData.summary?.total_students || 0}</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Avg Retention Rate</Text>
                    <Heading size="lg" color="green.500">{hsData.summary?.avg_retention_rate || 0}%</Heading>
                  </CardBody>
                </Card>
                <Card>
                  <CardBody>
                    <Text fontSize="sm" color="gray.600">Avg Graduation Rate</Text>
                    <Heading size="lg" color="blue.500">{hsData.summary?.avg_graduation_rate || 0}%</Heading>
                  </CardBody>
                </Card>
              </SimpleGrid>

              <Tabs colorScheme="blue">
                <TabList>
                  <Tab>Enrollment by High School</Tab>
                  <Tab>Retention & Graduation</Tab>
                  <Tab>Performance Analysis</Tab>
                  <Tab>Program Distribution</Tab>
                  <Tab>Tuition Completion</Tab>
                </TabList>

                <TabPanels>
                  <TabPanel>
                    <Card>
                      <CardBody>
                        <ResponsiveContainer width="100%" height={400}>
                          <BarChart data={hsData.data?.slice(0, 20)}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="high_school" angle={-45} textAnchor="end" height={100} />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Bar dataKey="total_students" fill="#3182CE" name="Total Students" />
                            <Bar dataKey="enrolled_students" fill="#38A169" name="Enrolled" />
                          </BarChart>
                        </ResponsiveContainer>
                      </CardBody>
                    </Card>
                  </TabPanel>
                  <TabPanel>
                    <Card>
                      <CardBody>
                        <ResponsiveContainer width="100%" height={400}>
                          <LineChart data={hsData.data?.slice(0, 20)}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="high_school" angle={-45} textAnchor="end" height={100} />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Line type="monotone" dataKey="retention_rate" stroke="#38A169" name="Retention Rate %" />
                            <Line type="monotone" dataKey="graduation_rate" stroke="#3182CE" name="Graduation Rate %" />
                            <Line type="monotone" dataKey="dropout_rate" stroke="#E53E3E" name="Dropout Rate %" />
                          </LineChart>
                        </ResponsiveContainer>
                      </CardBody>
                    </Card>
                  </TabPanel>
                  <TabPanel>
                    <Text>Performance analysis charts</Text>
                  </TabPanel>
                  <TabPanel>
                    <Text>Program distribution by high school</Text>
                  </TabPanel>
                  <TabPanel>
                    <Text>Tuition completion rates by high school</Text>
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

export default HighSchoolAnalytics;


