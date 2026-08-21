package artifactvalidator

func ffprobeArguments(inputPath string) []string {
	return ffprobeArgumentsWithProtocol(inputPath, "file")
}

func ffprobeArgumentsWithProtocol(inputPath string, protocol string) []string {
	return []string{
		"-v", "error",
		"-hide_banner",
		"-protocol_whitelist", protocol,
		"-probesize", "67108864",
		"-analyzeduration", "10000000",
		"-show_entries",
		"program_version=version:" +
			"stream=codec_name,codec_type,width,height,avg_frame_rate,nb_frames,duration:" +
			"format=format_name,duration,size",
		"-of", "json",
		inputPath,
	}
}
