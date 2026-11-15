function parseJson(bytes) { try { return JSON.parse(bytes.toString()); } catch { return null; } }
export function inspect(bytes) { return { format: 'AWS EventStream', firstFrameLength: bytes.length >= 4 ? bytes.readUInt32BE(0) : null, headerBytes: bytes.length >= 8 ? bytes.readUInt32BE(4) : null }; }
