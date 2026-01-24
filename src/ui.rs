use crate::fs::{FileSystem};
use crate::raidz::{UBERBLOCK_BLOCK_COUNT,UBERBLOCK_START,SUPERBLOCK_LBA,METASLAB_TABLE_START,SPACEMAP_LOG_BLOCKS_PER_METASLAB};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlockType {
    Superblock,
    Uberblock,
    MetaslabHeader,
    SpaceMapLog,
    FileIndex,
    FileData,
    Free,
}

impl BlockType {
    fn color(&self) -> &'static str {
        match self {
            BlockType::Superblock => "\x1b[48;5;196m",      // Bright red background
            BlockType::Uberblock => "\x1b[48;5;208m",       // Orange background
            BlockType::MetaslabHeader => "\x1b[48;5;226m",  // Yellow background
            BlockType::SpaceMapLog => "\x1b[48;5;220m",     // Gold background
            BlockType::FileIndex => "\x1b[48;5;39m",        // Bright blue background
            BlockType::FileData => "\x1b[48;5;34m",         // Green background
            BlockType::Free => "\x1b[48;5;240m",            // Dark gray background
        }
    }

    fn symbol(&self) -> &'static str {
        match self {
            BlockType::Superblock => "S",
            BlockType::Uberblock => "U",
            BlockType::MetaslabHeader => "H",
            BlockType::SpaceMapLog => "L",
            BlockType::FileIndex => "I",
            BlockType::FileData => "D",
            BlockType::Free => "·",
        }
    }

    fn name(&self) -> &'static str {
        match self {
            BlockType::Superblock => "Superblock",
            BlockType::Uberblock => "Uberblock",
            BlockType::MetaslabHeader => "Metaslab Headers",
            BlockType::SpaceMapLog => "Space Map Logs",
            BlockType::FileIndex => "File Index",
            BlockType::FileData => "File Data",
            BlockType::Free => "Free Space",
        }
    }
}

impl FileSystem {
    /// Get terminal dimensions, defaulting to 80x24 if detection fails
    fn get_terminal_dimensions() -> (usize, usize) {
        if let Some((w, h)) = term_size::dimensions() {
            (w, h)
        } else {
            (80, 24)
        }
    }

    /// Classify what type of block a given LBA represents
    fn classify_block(&self, lba: u64) -> BlockType {
        // Superblock
        if lba == SUPERBLOCK_LBA {
            return BlockType::Superblock;
        }

        // Uberblocks
        if lba >= UBERBLOCK_START && lba < UBERBLOCK_START + UBERBLOCK_BLOCK_COUNT {
            return BlockType::Uberblock;
        }

        let superblock = self.dev.superblock().unwrap();

        // Metaslab headers
        let metaslab_headers_start = METASLAB_TABLE_START;
        let metaslab_headers_end = metaslab_headers_start + superblock.metaslab_count as u64;
        if lba >= metaslab_headers_start && lba < metaslab_headers_end {
            return BlockType::MetaslabHeader;
        }

        // Space map logs
        let spacemap_logs_start = metaslab_headers_end;
        let spacemap_logs_end = spacemap_logs_start + 
            (superblock.metaslab_count as u64 * SPACEMAP_LOG_BLOCKS_PER_METASLAB);
        if lba >= spacemap_logs_start && lba < spacemap_logs_end {
            return BlockType::SpaceMapLog;
        }

        // File index
        for extent in &self.current_file_index_extent {
            if lba >= extent.start && lba < extent.start + extent.len {
                return BlockType::FileIndex;
            }
        }

        // File data
        for inode in self.file_index.inodes.values() {
            for extent in inode.inode_type.extents() {
                if lba >= extent.start && lba < extent.start + extent.len {
                    return BlockType::FileData;
                }
            }
        }

        // Check if free in any metaslab
        for ms in &self.metaslabs {
            for (&free_start, &free_len) in &ms.free_extents {
                if lba >= free_start && lba < free_start + free_len {
                    return BlockType::Free;
                }
            }
        }

        // If allocated but not referenced, it's technically allocated (not free)
        // We'll show it as FileData with a note
        BlockType::FileData
    }

    /// Build a complete block type map efficiently
    fn build_block_map(&self) -> Vec<BlockType> {
        let superblock = self.dev.superblock().unwrap();
        let total_blocks = superblock.total_blocks as usize;
        let mut block_map = vec![BlockType::FileData; total_blocks]; // Default to allocated
        
        // Mark free blocks first (since they're the base state for data region)
        for ms in &self.metaslabs {
            for (&free_start, &free_len) in &ms.free_extents {
                let start = free_start as usize;
                let end = (free_start + free_len) as usize;
                for i in start..end.min(total_blocks) {
                    block_map[i] = BlockType::Free;
                }
            }
        }
        
        // Mark file data
        for inode in self.file_index.inodes.values() {
            for extent in inode.inode_type.extents() {
                let start = extent.start as usize;
                let end = (extent.start + extent.len) as usize;
                for i in start..end.min(total_blocks) {
                    block_map[i] = BlockType::FileData;
                }
            }
        }
        
        // Mark file index
        for extent in &self.current_file_index_extent {
            let start = extent.start as usize;
            let end = (extent.start + extent.len) as usize;
            for i in start..end.min(total_blocks) {
                block_map[i] = BlockType::FileIndex;
            }
        }
        
        // Mark metadata regions (these override data allocations)
        let metaslab_headers_start = METASLAB_TABLE_START as usize;
        let metaslab_headers_end = metaslab_headers_start + superblock.metaslab_count as usize;
        for i in metaslab_headers_start..metaslab_headers_end.min(total_blocks) {
            block_map[i] = BlockType::MetaslabHeader;
        }
        
        let spacemap_logs_start = metaslab_headers_end;
        let spacemap_logs_end = spacemap_logs_start + 
            (superblock.metaslab_count as usize * SPACEMAP_LOG_BLOCKS_PER_METASLAB as usize);
        for i in spacemap_logs_start..spacemap_logs_end.min(total_blocks) {
            block_map[i] = BlockType::SpaceMapLog;
        }
        
        // Mark uberblocks
        let uberblock_start = UBERBLOCK_START as usize;
        let uberblock_end = (UBERBLOCK_START + UBERBLOCK_BLOCK_COUNT) as usize;
        for i in uberblock_start..uberblock_end.min(total_blocks) {
            block_map[i] = BlockType::Uberblock;
        }
        
        // Mark superblock
        if SUPERBLOCK_LBA < total_blocks as u64 {
            block_map[SUPERBLOCK_LBA as usize] = BlockType::Superblock;
        }
        
        block_map
    }

    /// Display a visual map of block usage
    pub fn display_block_map(&self) {
        let superblock = self.dev.superblock().unwrap();

        const RESET: &str = "\x1b[0m";
        
        let total_blocks = superblock.total_blocks;
        let (term_width, term_height) = Self::get_terminal_dimensions();
        
        // Reserve space for labels and margins (10 chars for block numbers + 3 for " │ " + 1 for safety)
        let available_width = term_width.saturating_sub(14);
        
        // Reserve space for headers, legend, statistics, and footer
        // Header: 4 lines, Legend: 9 lines, Stats: 2 lines, Footer: 3 lines = ~18 lines
        let available_height = term_height.saturating_sub(20).max(10);
        
        // Calculate total available characters in the grid
        let total_chars_available = available_width * available_height;
        
        // Calculate how many blocks each character should represent
        // We want to fit all blocks into the available grid
        let blocks_per_char = ((total_blocks as f64) / (total_chars_available as f64)).ceil() as u64;
        let blocks_per_char = blocks_per_char.max(1);
        
        println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
        println!("║                          RAIDZ BLOCK USAGE MAP                             ║");
        println!("╚════════════════════════════════════════════════════════════════════════════╝");
        println!();
        println!("Total Blocks: {}  |  Blocks per char: {}  |  Grid: {}x{} ({}x{} terminal)", 
                 total_blocks, blocks_per_char, available_width, available_height, term_width, term_height);
        println!();

        // Build the complete block map once
        let block_map = self.build_block_map();

        // Count blocks by type for statistics
        let mut counts = std::collections::HashMap::new();
        for &block_type in &block_map {
            *counts.entry(block_type).or_insert(0u64) += 1;
        }

        // Print legend
        println!("Legend:");
        for block_type in [
            BlockType::Superblock,
            BlockType::Uberblock,
            BlockType::MetaslabHeader,
            BlockType::SpaceMapLog,
            BlockType::FileIndex,
            BlockType::FileData,
            BlockType::Free,
        ] {
            let count = counts.get(&block_type).copied().unwrap_or(0);
            let percentage = (count as f64 / total_blocks as f64) * 100.0;
            println!("  {}{} {}{} - {:>6} blocks ({:>5.1}%)", 
                     block_type.color(),
                     block_type.symbol(),
                     RESET,
                     block_type.name(),
                     count,
                     percentage);
        }
        println!();

        // Print the map
        println!("Block Map:");
        println!();
        
        
        let mut block_idx = 0u64;
        
        while block_idx < total_blocks {
            // Print block number label
            print!("{:>10} │ ", block_idx);

            for _col in 0..available_width {
                if block_idx >= total_blocks {
                    break;
                }

                // Sample blocks in this range to determine dominant type
                let end_block = (block_idx + blocks_per_char).min(total_blocks);
                let mut type_counts = std::collections::HashMap::new();
                
                for b in block_idx..end_block {
                    let bt = block_map[b as usize];
                    *type_counts.entry(bt).or_insert(0) += 1;
                }

                // Find the most common block type in this range
                let dominant_type = type_counts.iter()
                    .max_by_key(|(_, count)| *count)
                    .map(|(bt, _)| *bt)
                    .unwrap_or(BlockType::Free);

                print!("{}{}{}", 
                       dominant_type.color(),
                       dominant_type.symbol(),
                       RESET);
                
                block_idx += blocks_per_char;
            }
            
            println!();
        }

        println!();

        // Print orphaned block warning if any exist
        let orphaned = self.find_orphaned_blocks();
        if !orphaned.is_empty() {
            let total_orphaned: u64 = orphaned.iter().map(|(_, len)| len).sum();
            println!("⚠️  WARNING: {} orphaned blocks detected in {} extents", 
                     total_orphaned, orphaned.len());
            if orphaned.len() <= 10 {
                println!("   Orphaned extents:");
                for (start, len) in &orphaned {
                    println!("     - Block {} (length {})", start, len);
                }
            }
        }

        println!();
    }

    /// Display detailed metaslab information
    pub fn display_metaslab_info(&self) {
        println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
        println!("║                         METASLAB INFORMATION                               ║");
        println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

        for (i, ms) in self.metaslabs.iter().enumerate() {
            let total_blocks = ms.header.block_count;
            let free_blocks: u64 = ms.free_extents.values().sum();
            let allocated_blocks = total_blocks - free_blocks;
            let utilization = (allocated_blocks as f64 / total_blocks as f64) * 100.0;
            let fragmentation = ms.free_extents.len();
            if allocated_blocks == 0 {
                continue;
            }

            println!("Metaslab #{}", i);
            println!("  Range: {} - {}", ms.header.start_block, 
                     ms.header.start_block + ms.header.block_count - 1);
            println!("  Total: {} blocks", total_blocks);
            println!("  Used:  {} blocks ({:.1}%)", allocated_blocks, utilization);
            println!("  Free:  {} blocks in {} extents", free_blocks, fragmentation);
            println!("  Space map: {} / {} log blocks used", 
                     ms.header.spacemap_blocks, SPACEMAP_LOG_BLOCKS_PER_METASLAB);
            
            // Draw a simple bar chart
            let bar_width = 50;
            let filled = ((utilization / 100.0) * bar_width as f64) as usize;
            print!("  [");
            for j in 0..bar_width {
                if j < filled {
                    print!("█");
                } else {
                    print!("░");
                }
            }
            println!("]");
            println!();
        }
    }
}