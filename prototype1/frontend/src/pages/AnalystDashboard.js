/**
 * Analyst Dashboard - Analytics Workspace
 */
import React, { useState } from 'react';
import {
  Box,
  Container,
  VStack,
  Heading,
  Text,
  Button,
  HStack,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react';
import { FaPlus, FaChartBar } from 'react-icons/fa';
import GlobalFilterPanel from '../components/GlobalFilterPanel';
import FEXAnalytics from './FEXAnalytics';
import HighSchoolAnalytics from './HighSchoolAnalytics';

const AnalystDashboard = () => {
  const [filters, setFilters] = useState({});

  return (
    <Box minH="100vh" bg="#F5F7FA">
      <Container maxW="container.xl" py={8}>
        <VStack spacing={6} align="stretch">
          <HStack justify="space-between">
            <Box>
              <Heading size="xl" color="blue.600">Analyst Workspace</Heading>
              <Text color="gray.600">Create and modify analytics dashboards</Text>
            </Box>
            <Button leftIcon={<FaPlus />} colorScheme="blue">
              New Dashboard
            </Button>
          </HStack>

          <GlobalFilterPanel onFilterChange={setFilters} />

          <Tabs>
            <TabList>
              <Tab>FEX Analytics</Tab>
              <Tab>High School Analytics</Tab>
              <Tab>Custom Analytics</Tab>
              <Tab>Saved Reports</Tab>
            </TabList>

            <TabPanels>
              <TabPanel>
                <FEXAnalytics />
              </TabPanel>
              <TabPanel>
                <HighSchoolAnalytics />
              </TabPanel>
              <TabPanel>
                <Text>Custom analytics builder</Text>
              </TabPanel>
              <TabPanel>
                <Text>Saved reports</Text>
              </TabPanel>
            </TabPanels>
          </Tabs>
        </VStack>
      </Container>
    </Box>
  );
};

export default AnalystDashboard;


