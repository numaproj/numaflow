import Box from "@mui/material/Box";
import CircleIcon from "@mui/icons-material/Circle";
import "./style.css";
interface EmptyChartProps {
  message?: string;
}

const EmptyChart = ({ message }: EmptyChartProps) => {
  return (
    <Box className={"empty_chart_container"}>
      <CircleIcon className={"circle_icon"} />

      <Box className={"empty_text"}>{message ? "Error occurred while retrieving data" : "No data for the selected filters."}</Box>
    </Box>
  );
};

export default EmptyChart;
