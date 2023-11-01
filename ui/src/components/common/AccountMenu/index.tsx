import React, { useCallback, useContext } from "react";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { useNavigate } from "react-router-dom";
import Chip from "@mui/material/Chip";

export default function AccountMenu() {
  const navigate = useNavigate();
  const { userInfo } = useContext<AppContextProps>(AppContext);

  const handleLogout = useCallback(async () => {
    try {
      const response = await fetch(`/auth/v1/logout`);
      if (response.ok) {
        navigate("/login");
      }
      // TODO on failure?
    } catch (e: any) {
      // TODO on failure?
    }
  }, []);

  if (!userInfo) {
    // Non-auth, hide account menu
    return undefined;
  }

  return (
    <React.Fragment>
      <Box
        sx={{ display: "flex", alignItems: "center", textAlign: "center" }}
        data-testid="account-menu"
      >
        <IconButton onClick={handleLogout} size="small" sx={{ ml: 2 }}>
          <Chip
            label="Log out"
            sx={{ backgroundColor: "#00D0E0", color: "#000" }}
          />
        </IconButton>
      </Box>
    </React.Fragment>
  );
}
