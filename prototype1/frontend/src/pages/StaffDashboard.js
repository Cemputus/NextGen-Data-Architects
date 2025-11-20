/**
 * Staff Dashboard - Personal Analytics + Class Management
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  HStack,
  Text,
  Input,
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
import { FaSearch, FaChalkboardTeacher } from 'react-icons/fa';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import axios from 'axios';

const StaffDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [classes, setClasses] = useState([]);
  const [selectedClass, setSelectedClass] = useState(null);
  const [studentSearch, setStudentSearch] = useState('');
  const [filters, setFilters] = useState({});

  useEffect(() => {
    loadStaffData();
  }, []);

  const loadStaffData = async () => {
    try {
      setLoading(true);
      const response = await axios.get('/api/analytics/staff/classes', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` }
      });
      setClasses(response.data.classes || []);
    } catch (err) {
      console.error('Error loading staff data:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <Heading size="xl" color="blue.600">Staff Dashboard</Heading>
          
          <Tabs>
            <TabList>
              <Tab>My Classes</Tab>
              <Tab>Class Analytics</Tab>
              <Tab>Student Search</Tab>
            </TabList>
            
            <TabPanels>
              <TabPanel>
                <SimpleGrid columns={{ base: 1, md: 3 }} spacing={4}>
                  {classes.map(cls => (
                    <Card key={cls.course_code} cursor="pointer" 
                          onClick={() => setSelectedClass(cls)}
                          borderWidth={selectedClass?.course_code === cls.course_code ? '2px' : '1px'}
                          borderColor={selectedClass?.course_code === cls.course_code ? 'blue.500' : 'gray.200'}>
                      <CardBody>
                        <Text fontWeight="bold">{cls.course_code}</Text>
                        <Text fontSize="sm" color="gray.600">{cls.course_name}</Text>
                        <Text fontSize="sm" mt={2}>{cls.student_count} students</Text>
                      </CardBody>
                    </Card>
                  ))}
                </SimpleGrid>
              </TabPanel>
              
              <TabPanel>
                {selectedClass && (
                  <Box>
                    <Text>Analytics for {selectedClass.course_code}</Text>
                    {/* Class analytics charts */}
                  </Box>
                )}
              </TabPanel>
              
              <TabPanel>
                <VStack spacing={4} align="stretch">
                  <HStack>
                    <Input
                      placeholder="Search by Access Number, Reg No, or Name"
                      value={studentSearch}
                      onChange={(e) => setStudentSearch(e.target.value)}
                    />
                    <Button leftIcon={<FaSearch />} colorScheme="blue">
                      Search
                    </Button>
                  </HStack>
                  {/* Student search results */}
                </VStack>
              </TabPanel>
            </TabPanels>
          </Tabs>
        </VStack>
      </Container>
    </Box>
  );
};

export default StaffDashboard;


