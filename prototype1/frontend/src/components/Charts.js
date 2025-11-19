import React from 'react';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import './Charts.css';

const COLORS = ['#667eea', '#764ba2', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#30cfd0'];

const Charts = ({ chartData }) => {
  const { departments, gradesOverTime, paymentStatus, attendance, gradeDistribution, topStudents } = chartData;

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
    <div className="charts-container">
      <div className="chart-row">
        <div className="chart-card">
          <h3>Students by Department</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={deptData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={80} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="students" fill="#667eea" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Average Grades Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={gradesData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="period" angle={-45} textAnchor="end" height={80} />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="grade" stroke="#764ba2" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="chart-row">
        <div className="chart-card">
          <h3>Payment Status Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={paymentData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {paymentData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Grade Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={gradeDistData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={80}
                paddingAngle={5}
                dataKey="value"
                label={({ name, value }) => `${name}: ${value}`}
              >
                {gradeDistData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="chart-row">
        <div className="chart-card">
          <h3>Attendance by Course (Top 8)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={attendanceData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="name" type="category" width={150} />
              <Tooltip />
              <Bar dataKey="hours" fill="#4facfe" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Top 10 Students</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={topStudentsData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} />
              <YAxis />
              <Tooltip />
              <Bar dataKey="grade" fill="#43e97b" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

export default Charts;




