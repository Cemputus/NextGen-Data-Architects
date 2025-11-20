/**
 * Prediction Page - Multi-model predictions with scenario analysis
 * Role-based access control
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  Container,
  VStack,
  HStack,
  Heading,
  Text,
  FormControl,
  FormLabel,
  Input,
  Select,
  Button,
  Card,
  CardBody,
  SimpleGrid,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  Alert,
  AlertIcon,
  Badge,
  Spinner,
  useToast,
} from '@chakra-ui/react';
import { FaChartLine, FaMagic, FaUsers, FaLightbulb } from 'react-icons/fa';
import axios from 'axios';
import { useAuth } from '../context/AuthContext';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const PredictionPage = () => {
  const { user } = useAuth();
  const [loading, setLoading] = useState(false);
  const [predictions, setPredictions] = useState(null);
  const [scenarios, setScenarios] = useState([]);
  const [selectedScenario, setSelectedScenario] = useState(null);
  const [studentIdentifier, setStudentIdentifier] = useState('');
  const [modelType, setModelType] = useState('ensemble');
  const toast = useToast();

  useEffect(() => {
    loadScenarios();
    // Pre-fill student identifier for students
    if (user?.role === 'student' && user?.access_number) {
      setStudentIdentifier(user.access_number);
    }
  }, []);

  const loadScenarios = async () => {
    try {
      const response = await axios.get('/api/predictions/scenarios', {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` }
      });
      setScenarios(response.data.scenarios || []);
    } catch (err) {
      console.error('Error loading scenarios:', err);
    }
  };

  const handlePredict = async () => {
    if (!studentIdentifier) {
      toast({
        title: 'Error',
        description: 'Please enter a student identifier',
        status: 'error',
        duration: 3000,
      });
      return;
    }

    setLoading(true);
    try {
      const response = await axios.post('/api/predictions/predict', {
        student_id: studentIdentifier,
        model_type: modelType
      }, {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` }
      });

      setPredictions({
        single: response.data,
        scenarios: null
      });

      toast({
        title: 'Prediction successful',
        description: `Predicted grade: ${response.data.predicted_grade}`,
        status: 'success',
        duration: 3000,
      });
    } catch (err) {
      toast({
        title: 'Error',
        description: err.response?.data?.error || 'Failed to generate prediction',
        status: 'error',
        duration: 3000,
      });
    } finally {
      setLoading(false);
    }
  };

  const handleScenarioPredict = async (scenario) => {
    if (!studentIdentifier) {
      toast({
        title: 'Error',
        description: 'Please enter a student identifier first',
        status: 'error',
        duration: 3000,
      });
      return;
    }

    setLoading(true);
    setSelectedScenario(scenario);
    
    try {
      const response = await axios.post('/api/predictions/scenario', {
        scenario: {
          base_student_id: studentIdentifier,
          ...scenario.parameters
        }
      }, {
        headers: { Authorization: `Bearer ${localStorage.getItem('token')}` }
      });

      setPredictions({
        single: null,
        scenarios: {
          scenario: scenario,
          predictions: response.data.predictions,
          analysis: response.data.analysis
        }
      });

      toast({
        title: 'Scenario analysis complete',
        status: 'success',
        duration: 3000,
      });
    } catch (err) {
      toast({
        title: 'Error',
        description: err.response?.data?.error || 'Failed to analyze scenario',
        status: 'error',
        duration: 3000,
      });
    } finally {
      setLoading(false);
    }
  };

  const canUseScenarios = ['analyst', 'sysadmin', 'senate'].includes(user?.role);

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          {/* Header */}
          <Box>
            <HStack spacing={3} mb={2}>
              <FaChartLine size={32} color="#3182CE" />
              <Heading size="xl" color="blue.600">Performance Prediction</Heading>
            </HStack>
            <Text color="gray.600">
              Predict student performance using multiple ML models
            </Text>
          </Box>

          {/* Prediction Form */}
          <Card>
            <CardBody>
              <VStack spacing={4} align="stretch">
                <HStack spacing={4}>
                  <FormControl flex={2}>
                    <FormLabel>
                      {user?.role === 'student' ? 'Your Access Number' : 'Student Identifier'}
                    </FormLabel>
                    <Input
                      value={studentIdentifier}
                      onChange={(e) => setStudentIdentifier(e.target.value)}
                      placeholder={user?.role === 'student' ? 'A#####' : 'Access Number, Reg No, or Student ID'}
                      isDisabled={user?.role === 'student'}
                    />
                    <Text fontSize="xs" color="gray.500" mt={1}>
                      {user?.role === 'student' 
                        ? 'You can only predict your own performance'
                        : 'Enter Access Number (A#####), Registration Number, or Student ID'}
                    </Text>
                  </FormControl>

                  <FormControl flex={1}>
                    <FormLabel>Model Type</FormLabel>
                    <Select value={modelType} onChange={(e) => setModelType(e.target.value)}>
                      <option value="ensemble">Ensemble (All Models)</option>
                      <option value="random_forest">Random Forest</option>
                      <option value="gradient_boosting">Gradient Boosting</option>
                      <option value="neural_network">Neural Network</option>
                    </Select>
                  </FormControl>

                  <Button
                    leftIcon={<FaMagic />}
                    colorScheme="blue"
                    onClick={handlePredict}
                    isLoading={loading}
                    loadingText="Predicting..."
                    mt={8}
                  >
                    Predict
                  </Button>
                </HStack>
              </VStack>
            </CardBody>
          </Card>

          {/* Results */}
          {predictions?.single && (
            <Card>
              <CardBody>
                <VStack spacing={4} align="stretch">
                  <Heading size="md">Prediction Results</Heading>
                  <SimpleGrid columns={{ base: 1, md: 3 }} spacing={4}>
                    <Card bg="blue.50">
                      <CardBody>
                        <Text fontSize="sm" color="gray.600">Predicted Grade</Text>
                        <Heading size="2xl" color="blue.600">
                          {predictions.single.predicted_grade}
                        </Heading>
                        <Badge colorScheme="blue" mt={2}>
                          {predictions.single.predicted_letter_grade}
                        </Badge>
                      </CardBody>
                    </Card>
                    <Card bg="green.50">
                      <CardBody>
                        <Text fontSize="sm" color="gray.600">Model Used</Text>
                        <Text fontSize="lg" fontWeight="bold" textTransform="capitalize">
                          {predictions.single.model_type.replace('_', ' ')}
                        </Text>
                      </CardBody>
                    </Card>
                    <Card bg="purple.50">
                      <CardBody>
                        <Text fontSize="sm" color="gray.600">Student ID</Text>
                        <Text fontSize="lg" fontWeight="bold">
                          {predictions.single.student_id}
                        </Text>
                      </CardBody>
                    </Card>
                  </SimpleGrid>
                </VStack>
              </CardBody>
            </Card>
          )}

          {/* Scenario Analysis */}
          {canUseScenarios && (
            <Tabs colorScheme="blue">
              <TabList>
                <Tab>Scenario Analysis</Tab>
                <Tab>Predefined Scenarios</Tab>
              </TabList>

              <TabPanels>
                <TabPanel>
                  {predictions?.scenarios && (
                    <VStack spacing={4} align="stretch">
                      <Card>
                        <CardBody>
                          <Heading size="md" mb={4}>
                            {predictions.scenarios.scenario.name}
                          </Heading>
                          <Text mb={4}>{predictions.scenarios.scenario.description}</Text>

                          <SimpleGrid columns={{ base: 1, md: 4 }} spacing={4} mb={4}>
                            {Object.entries(predictions.scenarios.predictions).map(([model, pred]) => (
                              <Card key={model} bg="blue.50">
                                <CardBody>
                                  <Text fontSize="sm" color="gray.600" textTransform="capitalize">
                                    {model.replace('_', ' ')}
                                  </Text>
                                  <Heading size="lg" color="blue.600">
                                    {pred.predicted_grade}
                                  </Heading>
                                  <Badge colorScheme="blue">{pred.predicted_letter_grade}</Badge>
                                </CardBody>
                              </Card>
                            ))}
                          </SimpleGrid>

                          {predictions.scenarios.analysis && (
                            <Alert
                              status={predictions.scenarios.analysis.risk_level === 'high' ? 'error' : 
                                     predictions.scenarios.analysis.risk_level === 'low' ? 'success' : 'warning'}
                            >
                              <AlertIcon />
                              <Box>
                                <Text fontWeight="bold">Risk Level: {predictions.scenarios.analysis.risk_level}</Text>
                                {predictions.scenarios.analysis.recommendations.map((rec, idx) => (
                                  <Text key={idx} fontSize="sm">• {rec}</Text>
                                ))}
                              </Box>
                            </Alert>
                          )}
                        </CardBody>
                      </Card>
                    </VStack>
                  )}
                </TabPanel>

                <TabPanel>
                  <SimpleGrid columns={{ base: 1, md: 2 }} spacing={4}>
                    {scenarios.map((scenario) => (
                      <Card key={scenario.id} cursor="pointer" 
                            onClick={() => handleScenarioPredict(scenario)}
                            _hover={{ boxShadow: 'lg' }}>
                        <CardBody>
                          <HStack justify="space-between" mb={2}>
                            <Heading size="sm">{scenario.name}</Heading>
                            <FaLightbulb color="#3182CE" />
                          </HStack>
                          <Text fontSize="sm" color="gray.600" mb={3}>
                            {scenario.description}
                          </Text>
                          <Button size="sm" colorScheme="blue" w="full">
                            Analyze Scenario
                          </Button>
                        </CardBody>
                      </Card>
                    ))}
                  </SimpleGrid>
                </TabPanel>
              </TabPanels>
            </Tabs>
          )}

          {!canUseScenarios && (
            <Alert status="info">
              <AlertIcon />
              Scenario analysis is only available for Analysts, System Administrators, and Senate members.
            </Alert>
          )}
        </VStack>
      </Container>
    </Box>
  );
};

export default PredictionPage;


