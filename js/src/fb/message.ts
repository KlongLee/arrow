// automatically generated by the FlatBuffers compiler, do not modify

import * as flatbuffers from 'flatbuffers';

import { KeyValue } from './key-value.js';
import { MessageHeader } from './message-header.js';
import { MetadataVersion } from './metadata-version.js';


export class Message {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
  __init(i:number, bb:flatbuffers.ByteBuffer):Message {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsMessage(bb:flatbuffers.ByteBuffer, obj?:Message):Message {
  return (obj || new Message()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsMessage(bb:flatbuffers.ByteBuffer, obj?:Message):Message {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new Message()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

version():MetadataVersion {
  const offset = this.bb!.__offset(this.bb_pos, 4);
  return offset ? this.bb!.readInt16(this.bb_pos + offset) : MetadataVersion.V1;
}

headerType():MessageHeader {
  const offset = this.bb!.__offset(this.bb_pos, 6);
  return offset ? this.bb!.readUint8(this.bb_pos + offset) : MessageHeader.NONE;
}

header(obj:any):any|null {
  const offset = this.bb!.__offset(this.bb_pos, 8);
  return offset ? this.bb!.__union(obj, this.bb_pos + offset) : null;
}

bodyLength():bigint {
  const offset = this.bb!.__offset(this.bb_pos, 10);
  return offset ? this.bb!.readInt64(this.bb_pos + offset) : BigInt('0');
}

customMetadata(index: number, obj?:KeyValue):KeyValue|null {
  const offset = this.bb!.__offset(this.bb_pos, 12);
  return offset ? (obj || new KeyValue()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
}

customMetadataLength():number {
  const offset = this.bb!.__offset(this.bb_pos, 12);
  return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
}

static startMessage(builder:flatbuffers.Builder) {
  builder.startObject(5);
}

static addVersion(builder:flatbuffers.Builder, version:MetadataVersion) {
  builder.addFieldInt16(0, version, MetadataVersion.V1);
}

static addHeaderType(builder:flatbuffers.Builder, headerType:MessageHeader) {
  builder.addFieldInt8(1, headerType, MessageHeader.NONE);
}

static addHeader(builder:flatbuffers.Builder, headerOffset:flatbuffers.Offset) {
  builder.addFieldOffset(2, headerOffset, 0);
}

static addBodyLength(builder:flatbuffers.Builder, bodyLength:bigint) {
  builder.addFieldInt64(3, bodyLength, BigInt('0'));
}

static addCustomMetadata(builder:flatbuffers.Builder, customMetadataOffset:flatbuffers.Offset) {
  builder.addFieldOffset(4, customMetadataOffset, 0);
}

static createCustomMetadataVector(builder:flatbuffers.Builder, data:flatbuffers.Offset[]):flatbuffers.Offset {
  builder.startVector(4, data.length, 4);
  for (let i = data.length - 1; i >= 0; i--) {
    builder.addOffset(data[i]!);
  }
  return builder.endVector();
}

static startCustomMetadataVector(builder:flatbuffers.Builder, numElems:number) {
  builder.startVector(4, numElems, 4);
}

static endMessage(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static finishMessageBuffer(builder:flatbuffers.Builder, offset:flatbuffers.Offset) {
  builder.finish(offset);
}

static finishSizePrefixedMessageBuffer(builder:flatbuffers.Builder, offset:flatbuffers.Offset) {
  builder.finish(offset, undefined, true);
}

static createMessage(builder:flatbuffers.Builder, version:MetadataVersion, headerType:MessageHeader, headerOffset:flatbuffers.Offset, bodyLength:bigint, customMetadataOffset:flatbuffers.Offset):flatbuffers.Offset {
  Message.startMessage(builder);
  Message.addVersion(builder, version);
  Message.addHeaderType(builder, headerType);
  Message.addHeader(builder, headerOffset);
  Message.addBodyLength(builder, bodyLength);
  Message.addCustomMetadata(builder, customMetadataOffset);
  return Message.endMessage(builder);
}
}
