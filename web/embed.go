package web

import "embed"

// StudioFS embeds the Web Studio dashboard assets.
//
//go:embed studio/*
var StudioFS embed.FS
