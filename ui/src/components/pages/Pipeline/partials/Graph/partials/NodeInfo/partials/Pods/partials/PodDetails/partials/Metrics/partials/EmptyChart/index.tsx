import Box from "@mui/material/Box";
import CircleIcon from "@mui/icons-material/Circle";

import "./style.css";

const EmptyChart = () => {
  return (
    <Box className={"empty_chart_container"}>
      <CircleIcon className={"circle_icon"} />

      <Box className={"empty_text"}>No data for the selected filters.</Box>
    </Box>
  );
};

export default EmptyChart;
