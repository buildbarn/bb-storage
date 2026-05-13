//go:build !linux

package program

func relaunchIfPID1(currentPID int, daemon bool) {}
