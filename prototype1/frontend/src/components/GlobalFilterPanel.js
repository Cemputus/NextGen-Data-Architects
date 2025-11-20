/**
 * Global Filter Panel Component
 * Provides comprehensive filtering options for analytics
 */
import React, { useState, useEffect } from 'react';
import {
  Box,
  VStack,
  HStack,
  Text,
  Select,
  Input,
  Button,
  IconButton,
  Collapse,
  useDisclosure,
  Badge,
  Wrap,
  WrapItem,
  FormControl,
  FormLabel,
  Checkbox,
  CheckboxGroup,
} from '@chakra-ui/react';
import { FaFilter, FaTimes, FaSearch, FaSave } from 'react-icons/fa';
import axios from 'axios';

const GlobalFilterPanel = ({ onFilterChange, savedFilters = [] }) => {
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: true });
  const [filters, setFilters] = useState({});
  const [filterOptions, setFilterOptions] = useState({
    faculties: [],
    departments: [],
    programs: [],
    courses: [],
    semesters: [],
    high_schools: [],
    intake_years: [],
  });
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    // Load filter options
    axios.get('/api/analytics/filter-options', {
      headers: { Authorization: `Bearer ${localStorage.getItem('token')}` }
    })
      .then(res => setFilterOptions(res.data))
      .catch(err => console.error('Error loading filter options:', err));
  }, []);

  const handleFilterChange = (key, value) => {
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    onFilterChange(newFilters);
  };

  const handleSearch = () => {
    // Search by Access Number, Reg Number, or Student Name
    if (searchTerm) {
      // Try to detect format
      if (/^[AB]\d{5}$/.test(searchTerm)) {
        handleFilterChange('access_number', searchTerm);
      } else if (/\d{2}[BMD]\d{2}\/\d{3}/.test(searchTerm)) {
        handleFilterChange('reg_number', searchTerm);
      } else {
        handleFilterChange('student_name', searchTerm);
      }
    }
  };

  const clearFilters = () => {
    setFilters({});
    setSearchTerm('');
    onFilterChange({});
  };

  const activeFiltersCount = Object.keys(filters).filter(k => filters[k]).length;

  return (
    <Box
      bg="white"
      borderWidth="1px"
      borderColor="gray.200"
      borderRadius="lg"
      p={4}
      mb={4}
      boxShadow="sm"
    >
      <HStack justify="space-between" mb={3}>
        <HStack>
          <FaFilter />
          <Text fontWeight="bold" fontSize="lg">Filters</Text>
          {activeFiltersCount > 0 && (
            <Badge colorScheme="blue">{activeFiltersCount} active</Badge>
          )}
        </HStack>
        <HStack>
          <IconButton
            icon={isOpen ? <FaTimes /> : <FaFilter />}
            onClick={onToggle}
            size="sm"
            variant="ghost"
            aria-label="Toggle filters"
          />
        </HStack>
      </HStack>

      <Collapse in={isOpen} animateOpacity>
        <VStack spacing={4} align="stretch">
          {/* Search Bar */}
          <HStack>
            <Input
              placeholder="Search by Access Number (A#####), Reg No, or Name"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
            />
            <Button
              leftIcon={<FaSearch />}
              onClick={handleSearch}
              colorScheme="blue"
            >
              Search
            </Button>
          </HStack>

          {/* Filter Controls */}
          <Wrap spacing={4}>
            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Faculty</FormLabel>
                <Select
                  placeholder="All Faculties"
                  value={filters.faculty_id || ''}
                  onChange={(e) => handleFilterChange('faculty_id', e.target.value || null)}
                >
                  {filterOptions.faculties?.map(f => (
                    <option key={f.faculty_id} value={f.faculty_id}>
                      {f.faculty_name}
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Department</FormLabel>
                <Select
                  placeholder="All Departments"
                  value={filters.department_id || ''}
                  onChange={(e) => handleFilterChange('department_id', e.target.value || null)}
                >
                  {filterOptions.departments?.map(d => (
                    <option key={d.department_id} value={d.department_id}>
                      {d.department_name}
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Program</FormLabel>
                <Select
                  placeholder="All Programs"
                  value={filters.program_id || ''}
                  onChange={(e) => handleFilterChange('program_id', e.target.value || null)}
                >
                  {filterOptions.programs?.map(p => (
                    <option key={p.program_id} value={p.program_id}>
                      {p.program_name}
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Course</FormLabel>
                <Select
                  placeholder="All Courses"
                  value={filters.course_code || ''}
                  onChange={(e) => handleFilterChange('course_code', e.target.value || null)}
                >
                  {filterOptions.courses?.map(c => (
                    <option key={c.course_code} value={c.course_code}>
                      {c.course_code} - {c.course_name}
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Semester</FormLabel>
                <Select
                  placeholder="All Semesters"
                  value={filters.semester_id || ''}
                  onChange={(e) => handleFilterChange('semester_id', e.target.value || null)}
                >
                  {filterOptions.semesters?.map(s => (
                    <option key={s.semester_id} value={s.semester_id}>
                      {s.semester_name}
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Intake Year</FormLabel>
                <Select
                  placeholder="All Years"
                  value={filters.intake_year || ''}
                  onChange={(e) => handleFilterChange('intake_year', e.target.value || null)}
                >
                  {filterOptions.intake_years?.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">High School</FormLabel>
                <Select
                  placeholder="All High Schools"
                  value={filters.high_school || ''}
                  onChange={(e) => handleFilterChange('high_school', e.target.value || null)}
                >
                  {filterOptions.high_schools?.map(hs => (
                    <option key={hs.high_school} value={hs.high_school}>
                      {hs.high_school} ({hs.high_school_district})
                    </option>
                  ))}
                </Select>
              </FormControl>
            </WrapItem>

            <WrapItem>
              <FormControl minW="200px">
                <FormLabel fontSize="sm">Gender</FormLabel>
                <Select
                  placeholder="All"
                  value={filters.gender || ''}
                  onChange={(e) => handleFilterChange('gender', e.target.value || null)}
                >
                  <option value="M">Male</option>
                  <option value="F">Female</option>
                </Select>
              </FormControl>
            </WrapItem>
          </Wrap>

          {/* Active Filters Display */}
          {activeFiltersCount > 0 && (
            <Box>
              <Text fontSize="sm" fontWeight="semibold" mb={2}>Active Filters:</Text>
              <Wrap>
                {Object.entries(filters).map(([key, value]) => {
                  if (!value) return null;
                  return (
                    <WrapItem key={key}>
                      <Badge colorScheme="blue" p={2}>
                        {key}: {value}
                        <IconButton
                          icon={<FaTimes />}
                          size="xs"
                          ml={2}
                          onClick={() => handleFilterChange(key, null)}
                          aria-label="Remove filter"
                        />
                      </Badge>
                    </WrapItem>
                  );
                })}
              </Wrap>
            </Box>
          )}

          {/* Action Buttons */}
          <HStack>
            <Button
              leftIcon={<FaTimes />}
              onClick={clearFilters}
              variant="outline"
              size="sm"
            >
              Clear All
            </Button>
            <Button
              leftIcon={<FaSave />}
              variant="outline"
              size="sm"
            >
              Save Filter Preset
            </Button>
          </HStack>
        </VStack>
      </Collapse>
    </Box>
  );
};

export default GlobalFilterPanel;


