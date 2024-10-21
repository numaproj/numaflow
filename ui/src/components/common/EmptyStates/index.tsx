export function EmptyChartState() {
    return (
      <div style={{ 
        display: 'flex', 
        flexDirection: 'column', 
        alignItems: 'center', 
        justifyContent: 'center', 
        height: '200px', // Adjust the height as needed
        border: '1px dashed #ccc', // Add a subtle border
        borderRadius: '5px',
        padding: '20px',
      }}>
        <svg 
          width="40" 
          height="40" 
          viewBox="0 0 24 24" 
          fill="none" 
          xmlns="http://www.w3.org/2000/svg"
          style={{ marginBottom: '15px', color: '#999' }} // Add margin and color
        >
          {/* Replace with your preferred SVG icon (e.g., a magnifying glass or empty box) */}
          <path 
            d="M12 2C6.48 2 2 6.48 2 12C2 17.52 6.48 22 12 22C17.52 22 22 17.52 22 12C22 6.48 17.52 2 12 2ZM15.59 7L12 10.59L8.41 7L7 8.41L10.59 12L7 15.59L8.41 17L12 13.41L15.59 17L17 15.59L13.41 12L17 8.41L15.59 7Z" 
            fill="currentColor" 
          />
        </svg>
   
        <p style={{ fontSize: '16px', color: '#666' }}>No data for the selected filters.</p>
      </div>
    );
  }