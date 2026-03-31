import React, { useState, useCallback } from "react";
import IconButton from "@mui/material/IconButton";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Tooltip from "@mui/material/Tooltip";
import LightModeIcon from "@mui/icons-material/LightMode";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import SettingsBrightnessIcon from "@mui/icons-material/SettingsBrightness";
import CheckIcon from "@mui/icons-material/Check";
import {
  useThemeContext,
  ThemePreference,
} from "../../../contexts/ThemeContext";

const themeOptions: { value: ThemePreference; label: string; icon: React.ReactNode }[] = [
  { value: "light", label: "Light", icon: <LightModeIcon fontSize="small" /> },
  { value: "dark", label: "Dark", icon: <DarkModeIcon fontSize="small" /> },
  { value: "system", label: "System", icon: <SettingsBrightnessIcon fontSize="small" /> },
];

export function ThemeToggle() {
  const { preference, resolvedTheme, setPreference } = useThemeContext();
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  const handleClick = useCallback((event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  }, []);

  const handleClose = useCallback(() => {
    setAnchorEl(null);
  }, []);

  const handleSelect = useCallback(
    (value: ThemePreference) => {
      setPreference(value);
      setAnchorEl(null);
    },
    [setPreference]
  );

  const tooltipText =
    preference === "system"
      ? `Theme: System (${resolvedTheme})`
      : `Theme: ${preference.charAt(0).toUpperCase() + preference.slice(1)}`;

  return (
    <>
      <Tooltip title={tooltipText}>
        <IconButton
          onClick={handleClick}
          size="small"
          sx={{ color: "#fff", ml: 1 }}
          aria-label="Theme settings"
          aria-controls={open ? "theme-menu" : undefined}
          aria-haspopup="true"
          aria-expanded={open ? "true" : undefined}
        >
          {resolvedTheme === "light" ? <LightModeIcon /> : <DarkModeIcon />}
        </IconButton>
      </Tooltip>
      <Menu
        id="theme-menu"
        anchorEl={anchorEl}
        open={open}
        onClose={handleClose}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "right",
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "right",
        }}
      >
        {themeOptions.map((option) => (
          <MenuItem
            key={option.value}
            onClick={() => handleSelect(option.value)}
            selected={preference === option.value}
          >
            <ListItemIcon>{option.icon}</ListItemIcon>
            <ListItemText>{option.label}</ListItemText>
            {preference === option.value && (
              <CheckIcon fontSize="small" sx={{ ml: 1 }} />
            )}
          </MenuItem>
        ))}
      </Menu>
    </>
  );
}
