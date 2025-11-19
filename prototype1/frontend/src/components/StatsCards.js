import React from 'react';
import './StatsCards.css';

const StatsCards = ({ stats }) => {
  if (!stats) return null;

  const cards = [
    { title: 'Total Students', value: stats.total_students, icon: '👥', color: '#667eea' },
    { title: 'Total Courses', value: stats.total_courses, icon: '📚', color: '#764ba2' },
    { title: 'Total Enrollments', value: stats.total_enrollments, icon: '📝', color: '#f093fb' },
    { title: 'Average Grade', value: `${stats.avg_grade}%`, icon: '⭐', color: '#4facfe' },
    { title: 'Total Payments', value: `UGX ${(stats.total_payments / 1000000).toFixed(1)}M`, icon: '💰', color: '#43e97b' },
    { title: 'Avg Attendance', value: `${stats.avg_attendance.toFixed(1)} hrs`, icon: '📊', color: '#fa709a' }
  ];

  return (
    <div className="stats-cards">
      {cards.map((card, index) => (
        <div key={index} className="stat-card" style={{ borderTopColor: card.color }}>
          <div className="stat-icon" style={{ color: card.color }}>
            {card.icon}
          </div>
          <div className="stat-content">
            <div className="stat-value">{card.value}</div>
            <div className="stat-title">{card.title}</div>
          </div>
        </div>
      ))}
    </div>
  );
};

export default StatsCards;




