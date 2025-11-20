/**
 * HR Dashboard
 */
import React from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  Text,
} from '@chakra-ui/react';

const HRDashboard = () => {
  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <Heading size="xl" color="blue.600">HR Dashboard</Heading>
          <Text>HR Analytics and Staff Management</Text>
        </VStack>
      </Container>
    </Box>
  );
};

export default HRDashboard;


