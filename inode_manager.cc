#include "inode_manager.h"
#define FILE_START(nblocks, ninodes) ((nblocks)/BPB + (ninodes)/IPB + 4)
#define GET_BITMAP_BIT(b, content) ((content[(b % BPB)/8] >> (b % 8)) & 1)
#define SET_BITMAP_BIT_ALLOC(b, content) (content[(b % BPB)/8] = (content[(b % BPB)/8] | (1 << (b % 8))))
#define SET_BITMAP_BIT_FREE(b, content) (content[(b % BPB)/8] = (content[(b % BPB)/8] & (~(1 << (b % 8))) ) )
#define BLOCK_FREE 0
#define BLOCK_ALLOC 1

#define DBG_GET_INODE1
#define DBG_PUT_INODE1
#define DBG_WRITE1
#define DBG_GETATTR1
#define DBG_ALLOC_BLOCKS1
// Comments for MACROs
// BBLOCK(b) : Block containing bit for block b
// The function returns which block should we look into,
// not whether the bit which tells block is free.
// Because BLOCK_SIZE * 8 = 4096 blocks recorded
// BLOCK_NUM = DISK_SIZE/BLOCK_SIZE = 32768 blocks on the disk
// So Free Block Table should be more than 1 block.
// Thus, size of (Bitmap for free block) is more than 1*BLOCK_SIZE.
// Which should be BLOCK_NUM/BPB blocks in total.
// '+2' has taken Boot block and Super block into account.
// in IBLOCK(i, nblocks)     ((nblocks)/BPB + (i)/IPB + 3)
// '+3' means get the inode block, if +2, then the block pointing
// to might be a Bitmap for free block.



// disk layer -----------------------------------------

disk::disk()
{
	bzero(blocks, sizeof(blocks));
}

void
	disk::read_block(blockid_t id, char *buf)
{
	if (id < 0 || id >= BLOCK_NUM || buf == NULL)
		return;

	memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
	disk::write_block(blockid_t id, const char *buf)
{
	if (id < 0 || id >= BLOCK_NUM || buf == NULL)
		return;

	memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
	block_manager::alloc_block()
{
	char buf[BLOCK_SIZE];
	bool found = false;
	//Check for every block and find the first free block
	blockid_t targetBlock = 0;
	blockid_t fileStart = FILE_START(sb.nblocks, sb.ninodes);
	for(blockid_t i = fileStart ; i < sb.nblocks ; i += BPB )
	{
		//load target bitmap
		read_block(BBLOCK(i), buf);
		//Search for a free block
		for(blockid_t j = 0 ; j < BPB  ; j++)
		{
			targetBlock = i + j;
			//All blocks have been searched, but no free block found.
			if(targetBlock >= sb.nblocks)
			{
				return 0;
			}
			//Check whether this block is free.
			int isFree = GET_BITMAP_BIT(targetBlock, buf);
			if(isFree == BLOCK_FREE)
			{
				//Update bitmap
                printf("[block_manager::alloc_block()] Block %d is free.\n", targetBlock);
				SET_BITMAP_BIT_ALLOC(targetBlock, buf);
				//Write changes back to disk

				write_block(BBLOCK(targetBlock),buf);
				char buf2[BLOCK_SIZE];
				//clear the disk content
				memset(buf2, 0, BLOCK_SIZE);
				write_block(targetBlock, buf2);
				found = true;
				break;
			}
		}
		if(found == true)
		{
			return targetBlock;
		}
	}
	return 0;
}

void
	block_manager::free_block(uint32_t id)
{

	char buf[BLOCK_SIZE];
	blockid_t fileStart = FILE_START(sb.nblocks, sb.ninodes);
	//Check if the block id is valid
	//If not, just return.
	if(id < fileStart || id >= sb.nblocks)
	{
		printf("[block_manager::free_block()] The block [%d] is invalid!\n", id);
		return ;
	}

	//load target bitmap
	read_block(BBLOCK(id), buf);
	//Check if this position is set to free
	int isFree = get_status(id);
	if(isFree == BLOCK_ALLOC)
	{
		//using_blocks.erase(it);
		char buf2[BLOCK_SIZE];
		//Clear contents
		memset(buf2, 0, BLOCK_SIZE);
		write_block(id, buf2);
		SET_BITMAP_BIT_FREE(id, buf);
		write_block(BBLOCK(id),buf);
	}
	else
	{
		printf("[block_manager::free_block()] The block [%d] is not Allocated!\n", id);
		return ;
	}
	return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
// <-free block bitmap-> and <-inode table-> are
// both a set of blocks, not one block.
// nblocks : There are totally [nblocks] blocks on the disk
// ninodes : There are totally [ninodes] inodes on the disk
block_manager::block_manager()
{

	d = new disk();

	// format the disk
	sb.size = BLOCK_SIZE * BLOCK_NUM;
	sb.nblocks = BLOCK_NUM;
	sb.ninodes = INODE_NUM;
	blockid_t targetBlock = 0;
	bool done = false;

	char buf[BLOCK_SIZE];
	memset(buf, 0, BLOCK_SIZE);
	blockid_t inodeStart = IBLOCK(0, sb.nblocks);
	//Set the status of prologue blocks to ALLOCATED
	for(blockid_t i = 0 ; i <= inodeStart ; i += BPB )
	{
		for(blockid_t j = 0 ; j < BPB  ; j++)
		{
			targetBlock = i + j;
			//Super block and Bitmap block have been set to Free.
			if(targetBlock > inodeStart)
			{
				done = true;
				break;
			}
			//Set super block and bitmap blocks to ALLOCATED
			SET_BITMAP_BIT_ALLOC(targetBlock, buf);
		}
		write_block(BBLOCK(i),buf);
		if(done == true)
		{
			break;
		}
	}


}

int block_manager::get_status(blockid_t block_id)
{
	blockid_t targetBBlock = BBLOCK(block_id);
	char buf[BLOCK_SIZE];
	d->read_block(targetBBlock, buf);
	int isFree = GET_BITMAP_BIT(block_id, buf);
	return isFree;
}


void
	block_manager::read_block(uint32_t id, char *buf)
{
	d->read_block(id, buf);
}

void
	block_manager::write_block(uint32_t id, const char *buf)
{
	d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
	bm = new block_manager();
	uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
	if (root_dir != 1) {
		printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
		exit(0);
	}
}

/* Create a new file.
* Return its inum. */

uint32_t
	inode_manager::alloc_inode(uint32_t type)
{

	if(type != extent_protocol::T_DIR && type != extent_protocol::T_FILE && type != extent_protocol::T_SYMLINK)
	{
		printf("[inode_manager::alloc_inode()]  Input file type error with [%d]\n", type);
		return 0;
	}

	uint32_t inodeNum = 0;
	bool done = false;

	for(uint32_t i = 1 ; i < bm->sb.ninodes ; i++)
	{
		inode_t* tempNode = get_inode(i);
		if(tempNode == NULL || tempNode->type == 0)
		{
			//Create new inode and write back to disk
			inodeNum = i;
			tempNode->type = type;
			tempNode->size = 0;
			tempNode->ctime = getCurrentTime();

			put_inode(i, tempNode);
			done = true;
			break;
		}
	}
	if(done == false)
	{
		printf("[inode_manager::alloc_inode()] No inode available\n");
		return 0;
	}
	return inodeNum;
}

void
	inode_manager::free_inode(uint32_t inum)
{
	if(inum >= bm->sb.ninodes)
	{
		printf("[inode_manager::free_inode()] Node [%d] is out of range\n", inum);
		return ;
	}

	inode_t *ino = get_inode(inum);
	if(ino == NULL)
	{
		printf("[inode_manager::free_inode()] Node [%d] is invalid\n", inum);
	}

	//A free block found
	if(ino->type == 0)
	{
		printf("[inode_manager::free_inode()] inode [%d] is not allocated!\n", inum);
	}
	else
	{
		ino->type = 0;
		put_inode(inum, ino);
	}
	free(ino);
	return ;
}


/* Return an inode structure by inum, NULL otherwise.
* Caller should release the memory. */
struct inode*
	inode_manager::get_inode(uint32_t inum)
{
	struct inode *ino, *ino_disk;
	char buf[BLOCK_SIZE];

	printf("\tim: get_inode %d\n", inum);

	if (inum < 0 || inum >= INODE_NUM) {
		printf("\tim: inum out of range\n");
		return NULL;
	}

	blockid_t targetIBlock = IBLOCK( inum, bm->sb.nblocks);

	bm->read_block(targetIBlock, buf);

	ino_disk = (struct inode*)buf + inum%IPB;
	ino_disk->atime = getCurrentTime();
	ino = (struct inode*)malloc(sizeof(struct inode));
	*ino = *ino_disk;
#ifdef DBG_GET_INODE
printf("\n\n========GET INODE================\n");
printf("inode %d: \n", inum);
printf("type=%d\n", ino->type);
printf("size=%d\n", ino->size);
printf("atime=%d\n", ino->atime);
printf("mtime=%d\n", ino->mtime);
printf("ctime=%d\n", ino->ctime);

if(ino->size > 0)
{
	uint32_t blocks = (ino->size - 1 ) / BLOCK_SIZE + 1;
	if(blocks < NDIRECT)
	{
		for(uint32_t i = 0 ; i < blocks ; i++)
		{
			printf("blocks[%d]=%d\n",i,  ino->blocks[i]);
		}
	}
	else
	{

		for(uint32_t i = 0 ; i < NDIRECT ; i++)
		{
			printf("blocks[%d]=%d\n",i,  ino->blocks[i]);
		}
		blockid_t test_buf[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char*)test_buf);
		for(uint32_t i = NDIRECT ; i < blocks ; i++)
		{
			printf("blocks[%d]=%d\n",i,  test_buf[i - NDIRECT]);
		}

	}
}
printf("==========GET INODE END==========\n");
#endif
	return ino;
}

void
	inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
	char buf[BLOCK_SIZE];
	memset(buf, 0, BLOCK_SIZE);
	struct inode *ino_disk;

	printf("\tim: put_inode %d\n", inum);
	if (ino == NULL)
		return;


	bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
	ino_disk = (struct inode*)buf + inum%IPB;

	*ino_disk = *ino;
    ino_disk->mtime = getCurrentTime();
	ino_disk->ctime = getCurrentTime();
#ifdef DBG_PUT_INODE
printf("\n\n========PUT INODE================\n");
printf("inode %d: \n", inum);
printf("type=%d\n", ino->type);
printf("size=%d\n", ino->size);
printf("atime=%d\n", ino->atime);
printf("mtime=%d\n", ino->mtime);
printf("ctime=%d\n", ino->ctime);
if(ino->size > 0)
{
	uint32_t blocks = (ino->size - 1 ) / BLOCK_SIZE + 1;
	if(blocks < NDIRECT)
	{
		for(uint32_t i = 0 ; i < blocks ; i++)
		{
			printf("blocks[%d]=%d\n",i,  ino->blocks[i]);
		}
	}
	else
	{

		for(uint32_t i = 0 ; i < NDIRECT ; i++)
		{
			printf("blocks[%d]=%d\n",i,  ino->blocks[i]);
		}
		blockid_t test_buf[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char*)test_buf);
		for(uint32_t i = NDIRECT ; i < blocks ; i++)
		{
			printf("blocks[%d]=%d\n",i,  test_buf[i - NDIRECT]);
		}

	}
}
printf("==========PUT INODE END==========\n");
#endif
	bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
* Return alloced data, should be freed by caller. */
void
	inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
	inode_t *fileNode = get_inode(inum);
	if(fileNode == NULL)
	{
		printf("[inode_manager::read_file()] File [%d] size is not valid!\n", inum);
		return ;
	}
	if(fileNode->type == 0)
	{
		printf("[inode_manager::read_file()] File [%d] is removed!\n", inum);
		return ;
	}
	uint32_t blocks = (fileNode->size - 1)/ BLOCK_SIZE + 1;
	if(fileNode->size == 0)
	{
		blocks = 0;
	}
	*size = fileNode->size;

	*buf_out = (char*)malloc(BLOCK_SIZE * blocks * sizeof(char));
	char buf[BLOCK_SIZE];
	if(blocks < 0)
	{
		printf("[inode_manager::read_file()] File [%d] size is not valid!\n", inum);
	}
	if(blocks <= NDIRECT)
	{
		for(uint32_t i = 0; i < blocks ; i++)
		{
			blockid_t curBlock = fileNode->blocks[i];
			if(bm->get_status(curBlock) == BLOCK_FREE)
			{
				printf("[inode_manager::read_file()] Block [%d] is not allocated!\n", curBlock);
			}
			bm->read_block(curBlock, (char*)(*buf_out + i * BLOCK_SIZE));
		}
	}
	else
	{
		for(uint32_t i = 0 ; i < NDIRECT ; i++)
		{
			blockid_t curBlock = fileNode->blocks[i];
			if(bm->get_status(curBlock) == BLOCK_FREE)
			{
				printf("[inode_manager::read_file()] Block [%d] is not allocated!\n", curBlock);
			}
			bm->read_block(curBlock, (char*)(*buf_out + i * BLOCK_SIZE));
		}
		bm->read_block(fileNode->blocks[NDIRECT], buf);
		for(uint32_t i = NDIRECT ; i < blocks ; i++)
		{
			blockid_t curBlock = 	*((blockid_t*)buf + i - NDIRECT);
			if(bm->get_status(curBlock) == BLOCK_FREE)
			{
				printf("[inode_manager::read_file()] Block [%d] is not allocated!\n", curBlock);
			}
			bm->read_block(curBlock, (char*)(*buf_out + i * BLOCK_SIZE));
		}
	}

	free(fileNode);
	return;
}

/* alloc/free blocks if needed */
void
	inode_manager::write_file(uint32_t inum, const char *buf, int size)

{
	inode *fileNode = get_inode(inum);
	if(fileNode == NULL)
	{
		printf("[inode_manager::write_file()] Old inode [%d] is not valid!\n", inum);
		return ;
	}

	blockid_t oldBlocks = (fileNode->size - 1)/ BLOCK_SIZE + 1;
	if(fileNode->size == 0)
	{
		oldBlocks = 0;
	}


	blockid_t newBlocks = (size - 1 ) / BLOCK_SIZE + 1;
	if(size == 0)
	{
		newBlocks = 0;
	}
#ifdef DBG_WRITE
	printf("[inode_manager::write_file()] fileNode->size=%d, size=%d\n", fileNode->size, size);
	printf("[inode_manager::write_file()] oldBlocks=%d, newBlocks=%d\n", oldBlocks, newBlocks);
#endif
	fileNode->size = size;
	if(oldBlocks < 0)
	{
		printf("[inode_manager::write_file()] Old file [%d] size is not valid!\n", inum);
		return ;
	}
	if(newBlocks < 0)
	{
		printf("[inode_manager::write_file()] New file [%d] size is not valid!\n", inum);
		return ;
	}
	if(newBlocks <= NDIRECT)
	{
		if(newBlocks < oldBlocks)
		{
			if(oldBlocks <= NDIRECT) // new < old < NDIRECT
			{
#ifdef DBG_WRITE
                printf("new < old < NDIRECT");
#endif
				alloc_blocks(0, newBlocks, (blockid_t*)(fileNode->blocks), buf);
				free_blocks(newBlocks, oldBlocks, (blockid_t*)(fileNode->blocks));
			}
			else// new < NDIRECT < old
			{
				//Free blocks in Double Indirect Block List
#ifdef DBG_WRITE
                printf("new < NDIRECT < old");
#endif
				blockid_t buf_nodes[NINDIRECT];
				bm->read_block(fileNode->blocks[NDIRECT], (char*)buf_nodes);  //Load double indirect block into memory
				free_blocks(0, oldBlocks - NDIRECT, buf_nodes);	//Free target blocks in double indirect blocks
				bm->free_block(fileNode->blocks[NDIRECT]); //Free double indirect block
				alloc_blocks(0, newBlocks, (blockid_t*)(fileNode->blocks), buf);
				free_blocks(newBlocks, NDIRECT, fileNode->blocks);
			}
		}
		else // old <= new <= NDIRECT
		{
#ifdef DBG_WRITE
            printf("old <= new <= NDIRECT");
#endif
			for(blockid_t i = oldBlocks ; i < newBlocks ; i++)
			{
				fileNode->blocks[i] = bm->alloc_block();
#ifdef DBG_WRITE
                printf("[inode_manager::write_file()] Allocating blocks[%d] to %d on disk.\n", i, fileNode->blocks[i]);
#endif
				if(fileNode->blocks[i] == 0)
				{
					printf("[inode_manager::write_file()] No free space!\n");
					break;
				}
			}
			blockid_t tempBlocks[NDIRECT + 1];
			for(blockid_t i = 0 ; i < newBlocks ; i++)
			{
				tempBlocks[i] = fileNode->blocks[i];
#ifdef DBG_WRITE
				printf("fileNode->blocks[i] = %d\n", fileNode->blocks[i]);
#endif
			}
			alloc_blocks(0, newBlocks, tempBlocks, buf);
		}
	}
	else
	{
		if(oldBlocks <= NDIRECT )	//old <= NDIRECT  < new
		{
#ifdef DBG_WRITE
            printf("old <= NDIRECT  < new");
#endif
			for(blockid_t i = oldBlocks ; i <= NDIRECT ; i++)
			{
				fileNode->blocks[i] = bm->alloc_block();
				if(fileNode->blocks[i] == 0)
				{
					printf("[inode_manager::write_file()] No free space!\n");
					break;
				}
			}
			blockid_t DIBlock[NINDIRECT];
			for(blockid_t i = 0 ; i < newBlocks - oldBlocks ; i++ )
			{
				blockid_t curBlock = bm->alloc_block();
				if(curBlock == 0)
				{
					printf("[inode_manager::write_file()] No free space!\n");
					break;
				}
				DIBlock[i] = curBlock;
			}
			bm->write_block(fileNode->blocks[NDIRECT], (char*)DIBlock);
			alloc_blocks(0, NDIRECT, (blockid_t*)(fileNode->blocks), buf);
			alloc_blocks(NDIRECT, newBlocks, DIBlock, buf);
		}
		else
		{
			if(oldBlocks < newBlocks) //NDIRECT < old < new
			{
#ifdef DBG_WRITE
                printf("NDIRECT < old < new");
#endif
				alloc_blocks(0, NDIRECT, (blockid_t*)(fileNode->blocks), buf);
				blockid_t DIBlock[NINDIRECT];
                bm->read_block(fileNode->blocks[NDIRECT], (char*)DIBlock);
				for(blockid_t i = oldBlocks - NDIRECT ; i < newBlocks - NDIRECT; i++ )
				{
					blockid_t curBlock = bm->alloc_block();
					if(curBlock == 0)
					{
						printf("[inode_manager::write_file()] No free space!\n");
						break;
					}
					DIBlock[i] = curBlock;
				}
				bm->write_block(fileNode->blocks[NDIRECT], (char*)DIBlock);
				alloc_blocks(NDIRECT, newBlocks, DIBlock, buf);
			}
			else	//NDIRECT < new < old
			{
#ifdef DBG_WRITE
                printf("NDIRECT < new < old");
#endif
				alloc_blocks(0, NDIRECT, fileNode->blocks, buf);
				blockid_t DIBlock[NINDIRECT];
				bm->read_block(fileNode->blocks[NDIRECT], (char*)DIBlock);
				alloc_blocks(NDIRECT, newBlocks, DIBlock, buf);
				free_blocks(newBlocks - NDIRECT, oldBlocks - NDIRECT, DIBlock);
			}
		}
	}
	put_inode(inum, fileNode);
	free(fileNode);
	return;
}

void
	inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
#ifdef DBG_GETATTR
	printf("[inode_manager::getattr()] Get inum=%d\n", inum);
#endif
	inode_t *fileNode = get_inode(inum);
	if(fileNode == NULL)
	{
		printf("[inode_manager::getattr()] File [%d] is not valid!\n", inum);
		return ;
	}
	a.atime = fileNode->atime;
	a.mtime = fileNode->mtime;
	a.ctime = fileNode->ctime;
	a.size = fileNode->size;
	a.type = fileNode->type;
	return;
}

void
	inode_manager::remove_file(uint32_t inum)
{
	inode_t *fileNode = get_inode(inum);
	if(fileNode == NULL)
	{
		printf("[inode_manager::remove_file()] File [%d] is not valid!\n", inum);
		return ;
	}
	uint32_t blocks = fileNode->size / BLOCK_SIZE + 1;
	if(fileNode->size == 0)
	{
		blocks = 0;
	}
	if(blocks < NDIRECT)
	{
		free_blocks(0,blocks, (blockid_t*)(fileNode->blocks));
	}
	else
	{
		free_blocks(0, NDIRECT, (blockid_t*)(fileNode->blocks));
		blockid_t buf_nodes[NINDIRECT];
		bm->read_block(fileNode->blocks[NDIRECT], (char*)buf_nodes);
	}
	free(fileNode);
	free_inode(inum);
	return;
}


void inode_manager::free_blocks(blockid_t from, blockid_t to,  blockid_t *blocks)
{
	for(blockid_t i = from ; i < to ; i++)
	{
		blockid_t curBlock = blocks[i];
		if(bm->get_status(curBlock) == BLOCK_FREE)
		{
			printf("[inode_manager::free_blocks()] Block [%d] size is not allocated!\n", curBlock);
			continue;
		}
		bm->free_block(curBlock);
	}
}

void inode_manager::alloc_blocks(blockid_t from, blockid_t to, blockid_t* blocks, const char *buf)
{
#ifdef DBG_ALLOC_BLOCKS
	printf("[inode_manager::alloc_blocks()] from=%d, to=%d", from, to);
#endif
	char* buf2;
#ifdef DBG_ALLOC_BLOCKS
	for(blockid_t i = from ; i < to ; i++)
	{
		printf("blocks[i] = %d\n", blocks[i]);
	}
#endif
	int offset = 0;
	if(from == NDIRECT)
	{
		from -= NDIRECT;
		to -= NDIRECT;
		offset = NDIRECT;
	}
#ifdef DBG_ALLOC_BLOCKS
	printf("[inode_manager::alloc_blocks()] from=%d, to=%d, offset=%d\n", from, to, offset);
#endif
	for(blockid_t i = from ; i < to ; i++)
	{
#ifdef DBG_ALLOC_BLOCKS
		printf("i=%d, blocks[i] = %d\n", i, blocks[i]);
#endif
		buf2 = (char*)malloc(BLOCK_SIZE);
		memcpy(buf2, (char*)(buf + (i + offset) * BLOCK_SIZE),  BLOCK_SIZE);
		if(bm->get_status(blocks[i]) == BLOCK_FREE)
		{
			printf("[inode_manager::alloc_blocks()] Block [%d] is not allocated.\n", blocks[i]);
			continue;
		}
		bm->write_block(blocks[i], buf2);
		free(buf2);
	}
#ifdef DBG_ALLOC_BLOCKS
	printf("[inode_manager::alloc_blocks()] done\n");
#endif
}

unsigned int inode_manager::getCurrentTime()
{
	struct timeval start1;
	gettimeofday(&start1,0);
 	return start1.tv_sec * 1000 + start1.tv_usec;
}
