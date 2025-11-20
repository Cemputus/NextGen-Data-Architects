import React from 'react';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import {
  SimpleGrid,
  Card,
  CardBody,
  Heading,
  Box,
} from '@chakra-ui/react';

// Different color themes for different charts
const DEPT_COLORS = ['#2196F3', '#42A5F5', '#64B5F6', '#90CAF9', '#BBDEFB', '#E3F2FD'];
const PAYMENT_COLORS = ['#4CAF50', '#66BB6A', '#81C784', '#A5D6A7', '#C8E6C9'];
const GRADE_COLORS = ['#9C27B0', '#BA68C8', '#CE93D8', '#E1BEE7', '#F3E5F5'];
const ATTENDANCE_COLORS = ['#FF9800', '#FFB74D', '#FFCC80', '#FFE0B2', '#FFF3E0'];
const STUDENT_COLORS = ['#F44336', '#EF5350', '#E57373', '#EF9A9A', '#FFCDD2'];

const Charts = ({ chartData = {} }) => {
  // Safely destructure with defaults
  const { 
    departments = {}, 
    gradesOverTime = {}, 
    paymentStatus = {}, 
    attendance = {}, 
    gradeDistribution = {}, 
    topStudents = {} 
  } = chartData || {};

  // Prepare data for charts
  const deptData = departments?.departments?.map((dept, idx) => ({
    name: dept,
    students: departments.counts[idx]
  })) || [];

  const gradesData = gradesOverTime?.periods?.map((period, idx) => ({
    period,
    grade: gradesOverTime.grades[idx]
  })) || [];

  const paymentData = paymentStatus?.statuses?.map((status, idx) => ({
    name: status,
    value: paymentStatus.counts[idx]
  })) || [];

  const attendanceData = attendance?.courses?.slice(0, 8).map((course, idx) => ({
    name: course.length > 20 ? course.substring(0, 20) + '...' : course,
    hours: attendance.avg_hours[idx]
  })) || [];

  const gradeDistData = gradeDistribution?.grades?.map((grade, idx) => ({
    name: grade,
    value: gradeDistribution.counts[idx]
  })) || [];

  const topStudentsData = topStudents?.students?.map((student, idx) => ({
    name: student.length > 15 ? student.substring(0, 15) + '...' : student,
    grade: topStudents.grades[idx]
  })) || [];

  return (
    <Box mb={8}>
      <SimpleGrid columns={{ base: 1, lg: 2 }} spacing={6} mb={6}>
        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#2196F3" fontWeight="600">
              Students by Department
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={deptData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="name" angle={-45} textAnchor="end" height={80} stroke="#64748b" />
                  <YAxis stroke="#64748b" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                  <Bar dataKey="students" fill="#2196F3" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>

        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#9C27B0" fontWeight="600">
              Average Grades Over Time
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={gradesData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="period" angle={-45} textAnchor="end" height={80} stroke="#64748b" />
                  <YAxis stroke="#64748b" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                  <Legend />
                  <Line 
                    type="monotone" 
                    dataKey="grade" 
                    stroke="#9C27B0" 
                    strokeWidth={3}
                    dot={{ fill: '#9C27B0', r: 4 }}
                    activeDot={{ r: 6 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>
      </SimpleGrid>

      <SimpleGrid columns={{ base: 1, lg: 2 }} spacing={6} mb={6}>
        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#4CAF50" fontWeight="600">
              Payment Status Distribution
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={paymentData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {paymentData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={PAYMENT_COLORS[index % PAYMENT_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                </PieChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>

        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#9C27B0" fontWeight="600">
              Grade Distribution
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={gradeDistData}
                    cx="50%"
                    cy="50%"
                    innerRadius={70}
                    outerRadius={100}
                    paddingAngle={5}
                    dataKey="value"
                    label={({ name, value }) => `${name}: ${value}`}
                  >
                    {gradeDistData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={GRADE_COLORS[index % GRADE_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                </PieChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>
      </SimpleGrid>

      <SimpleGrid columns={{ base: 1, lg: 2 }} spacing={6}>
        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#FF9800" fontWeight="600">
              Attendance by Course (Top 8)
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={attendanceData} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis type="number" stroke="#64748b" />
                  <YAxis dataKey="name" type="category" width={150} stroke="#64748b" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                  <Bar dataKey="hours" fill="#FF9800" radius={[0, 8, 8, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>

        <Card boxShadow="sm" borderRadius="lg" bg="white" border="1px" borderColor="gray.100">
          <CardBody p={6}>
            <Heading size="md" mb={4} color="#F44336" fontWeight="600">
              Top 10 Students
            </Heading>
            <Box h="300px">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topStudentsData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} stroke="#64748b" />
                  <YAxis stroke="#64748b" />
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'white', 
                      border: '1px solid #e2e8f0',
                      borderRadius: '8px'
                    }} 
                  />
                  <Bar dataKey="grade" fill="#F44336" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Box>
          </CardBody>
        </Card>
      </SimpleGrid>
    </Box>
  );
};

export default Charts;




