import React, { useEffect, useState } from "react";
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
import { getRelativeTime } from "../../../../../../../../../../../../../../../utils";
import { ErrorDetails } from "../../../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

interface ContainerDropdownProps {
  container: string;
  details?: ErrorDetails[];
  onRefresh: () => void;
}

export const ContainerDropdown = ({
  container,
  details,
  onRefresh,
}: ContainerDropdownProps) => {
  const [expanded, setExpanded] = useState(false);
  const [errorsCount, setErrorsCount] = useState<number>(0);
  const [sortedDetails, setSortedDetails] = useState<
    ErrorDetails[] | undefined
  >(undefined);
  const [lastOccurred, setLastOccurred] = useState<string | undefined>(
    undefined
  );

  useEffect(() => {
    const count = details?.length || 0;
    setErrorsCount(count);

    const sorted = details
      ? [...details].sort(
          (a, b) =>
            new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
        )
      : undefined;
    setSortedDetails(sorted);

    setLastOccurred(sorted?.[0]?.timestamp);
  }, [details]);

  const handleChange =
    (panel: boolean) => (_event: React.SyntheticEvent, isExpanded: boolean) => {
      setExpanded(isExpanded ? panel : false);
      onRefresh();
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
            gap: "0.5rem",
          },
        }}
      >
        <Divider className={"divider-left-error-container"} />
        <Box className={"error-container-box"}>
          <Box className={"error-container-name-text"}>{container}</Box>
          {errorsCount > 0 && (
            <Box className={"error-container-count-text"}>{errorsCount}</Box>
          )}
        </Box>
        <Divider className={"divider-middle-error-container"} />
        {lastOccurred && (
          <Box className={"error-container-name-text"}>
            {getRelativeTime(lastOccurred)}
          </Box>
        )}
        {lastOccurred && (
          <Divider className={"divider-right-error-container"} />
        )}
      </AccordionSummary>
      {sortedDetails?.length ? (
        <AccordionDetails>
          <Box className={"error-container-acc-box"}>
            <TableContainer component={Paper}>
              <Table aria-label="error details table">
                <TableHead>
                  <TableRow sx={{ display: "flex" }}>
                    <TableCell
                      className={"error-container-table-header"}
                      sx={{ width: "18rem" }}
                    >
                      Last Occurred
                    </TableCell>
                    <TableCell
                      align="left"
                      className={"error-container-table-header"}
                      sx={{ flex: 1 }}
                    >
                      Message
                    </TableCell>
                    <TableCell
                      align="left"
                      className={"error-container-table-header"}
                      sx={{ flex: 1 }}
                    >
                      Details
                    </TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {sortedDetails?.map((row, index) => (
                    <TableRow key={index} sx={{ display: "flex" }}>
                      <TableCell
                        component="th"
                        scope="row"
                        className={"error-container-table-values"}
                        sx={{ width: "18rem" }}
                      >
                        <Box>
                          <Box>{getRelativeTime(row.timestamp)}</Box>
                          <Box sx={{ fontSize: "1.2rem" }}>{row.timestamp}</Box>
                        </Box>
                      </TableCell>
                      <TableCell
                        align="left"
                        className={"error-container-table-values"}
                        sx={{ flex: 1, overflow: "scroll" }}
                      >
                        {row.message}
                      </TableCell>
                      <TableCell
                        align="left"
                        className={"error-container-table-values"}
                        sx={{ flex: 1, overflow: "scroll" }}
                      >
                        {row.details}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        </AccordionDetails>
      ) : (
        <AccordionDetails>
          <Box className={"error-container-acc-box"}>
            No errors for this container
          </Box>
        </AccordionDetails>
      )}
    </Accordion>
  );
};
