import React, { useState } from "react";
import Box from "@mui/material/Box";
import Accordion from "@mui/material/Accordion";
import AccordionSummary from "@mui/material/AccordionSummary";
import AccordionDetails from "@mui/material/AccordionDetails";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Divider from "@mui/material/Divider";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import { ErrorDetails } from "../../index";

interface ContainerDropdownProps {
  container: string;
  details?: ErrorDetails[];
}

export const ContainerDropdown = ({
  container,
  details,
}: ContainerDropdownProps) => {
  const [expanded, setExpanded] = useState(false);

  const handleChange =
    (panel: boolean) => (_event: React.SyntheticEvent, isExpanded: boolean) => {
      setExpanded(isExpanded ? panel : false);
    };

  return (
    <Accordion
      sx={{
        boxShadow: "0",
      }}
      expanded={expanded}
      onChange={handleChange(true)}
    >
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        aria-controls="container-dropdown-content"
        id="container-dropdown-header"
        sx={{
          "& .MuiAccordionSummary-content": {
            display: "flex",
            alignItems: "center",
          },
        }}
      >
        <Divider
          sx={{ width: "5rem", marginRight: "1rem", backgroundColor: "black" }}
        />
        <Box sx={{ fontSize: "1.3rem", whiteSpace: "nowrap" }}>{container}</Box>
        <Divider
          sx={{ flexGrow: 1, marginLeft: "1rem", backgroundColor: "black" }}
        />
      </AccordionSummary>
      {details ? (
        <AccordionDetails>
          <Box sx={{ padding: "0.5rem 1rem" }}>
            <TableContainer component={Paper}>
              <Table aria-label="error details table">
                <TableHead>
                  <TableRow>
                    <TableCell>Timestamp</TableCell>
                    <TableCell align="left">Code</TableCell>
                    <TableCell align="left">Message</TableCell>
                    <TableCell align="left">Details</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {details.map((row, index) => (
                    <TableRow key={index}>
                      <TableCell component="th" scope="row">
                        {row.timestamp}
                      </TableCell>
                      <TableCell align="left">{row.code}</TableCell>
                      <TableCell align="left">{row.message}</TableCell>
                      <TableCell align="left">{row.details}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        </AccordionDetails>
      ) : (
        <AccordionDetails>
          <Box sx={{ padding: "0.5rem 1rem" }}>
            <Typography>Errors not found</Typography>
          </Box>
        </AccordionDetails>
      )}
    </Accordion>
  );
};
