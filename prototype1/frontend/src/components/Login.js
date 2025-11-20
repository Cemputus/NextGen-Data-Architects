import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Container,
  VStack,
  Heading,
  FormControl,
  FormLabel,
  Input,
  Button,
  Alert,
  AlertIcon,
  Text,
  Card,
  CardBody,
  Icon,
  HStack,
  Spinner,
  Center,
} from '@chakra-ui/react';
import { FaGraduationCap, FaUser, FaLock } from 'react-icons/fa';
import { useAuth } from '../context/AuthContext';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login, isAuthenticated, loading: authLoading } = useAuth();
  const navigate = useNavigate();

  // Redirect if already authenticated
  useEffect(() => {
    if (!authLoading && isAuthenticated) {
      navigate('/dashboard');
    }
  }, [isAuthenticated, authLoading, navigate]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    const result = await login(username, password);
    
    if (result.success) {
      // Redirect to role-specific dashboard
      const role = result.user?.role;
      const routes = {
        senate: '/senate/dashboard',
        sysadmin: '/admin/dashboard',
        analyst: '/analyst/dashboard',
        student: '/student/dashboard',
        staff: '/staff/dashboard',
        dean: '/dean/dashboard',
        hod: '/hod/dashboard',
        hr: '/hr/dashboard',
        finance: '/finance/dashboard',
      };
      navigate(routes[role] || '/dashboard');
    } else {
      setError(result.error);
    }
    
    setLoading(false);
  };

  // Show loading while checking authentication
  if (authLoading) {
    return (
      <Box minH="100vh" bgGradient="linear(to-br, blue.50, blue.100)">
        <Center minH="100vh">
          <Spinner size="xl" color="blue.500" thickness="4px" />
        </Center>
      </Box>
    );
  }

  // Don't show login if already authenticated (will redirect)
  if (isAuthenticated) {
    return null;
  }

  return (
    <Box
      minH="100vh"
      bgGradient="linear(to-br, blue.50, blue.100)"
      display="flex"
      alignItems="center"
      justifyContent="center"
      py={8}
    >
      <Container maxW="md">
        <Card boxShadow="2xl" borderRadius="2xl" overflow="hidden">
          <Box bg="blue.600" p={6} textAlign="center">
            <HStack justify="center" spacing={3} mb={2}>
              <Icon as={FaGraduationCap} boxSize={8} color="white" />
              <Heading size="lg" color="white">
                UCU Analytics
              </Heading>
            </HStack>
            <Text color="blue.100" fontSize="sm">
              Uganda Christian University
            </Text>
          </Box>
          
          <CardBody p={8}>
            <form onSubmit={handleSubmit}>
              <VStack spacing={6}>
                       <FormControl isRequired>
                         <FormLabel>
                           <HStack spacing={2}>
                             <Icon as={FaUser} color="blue.500" />
                             <Text>Access Number / Username / Email</Text>
                           </HStack>
                         </FormLabel>
                         <Input
                           type="text"
                           value={username}
                           onChange={(e) => setUsername(e.target.value)}
                           placeholder="Enter Access Number (A#####), Username, or Email"
                           size="lg"
                           focusBorderColor="blue.500"
                         />
                         <Text fontSize="xs" color="gray.500" mt={1}>
                           Students: Use Access Number (A##### or B#####)
                         </Text>
                       </FormControl>

                <FormControl isRequired>
                  <FormLabel>
                    <HStack spacing={2}>
                      <Icon as={FaLock} color="blue.500" />
                      <Text>Password</Text>
                    </HStack>
                  </FormLabel>
                  <Input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="Enter password"
                    size="lg"
                    focusBorderColor="blue.500"
                  />
                </FormControl>

                {error && (
                  <Alert status="error" borderRadius="md">
                    <AlertIcon />
                    {error}
                  </Alert>
                )}

                <Button
                  type="submit"
                  colorScheme="blue"
                  size="lg"
                  width="full"
                  isLoading={loading}
                  loadingText="Logging in..."
                >
                  Login
                </Button>

                <Box
                  bg="blue.50"
                  p={4}
                  borderRadius="md"
                  width="full"
                  borderWidth="1px"
                  borderColor="blue.200"
                >
                  <Text fontSize="sm" color="blue.700" textAlign="center">
                    <strong>Demo credentials:</strong> admin/admin123 or analyst/analyst123
                  </Text>
                </Box>
              </VStack>
            </form>
          </CardBody>
        </Card>
      </Container>
    </Box>
  );
};

export default Login;




