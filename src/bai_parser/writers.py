from collections import OrderedDict

from .constants import CONTINUATION_CODE
from .models import (
    Bai2File, Bai2FileHeader, Bai2FileTrailer,
    Group, GroupHeader, GroupTrailer,
    AccountIdentifier, AccountTrailer, Account,
    TransactionDetail
)
from .utils import write_date, write_time, convert_to_string


class BaseWriter:
    def __init__(self, obj, line_length=80, text_on_new_line=False, clock_format_for_intra_day=False):
        self.obj = obj
        self.line_length = line_length
        self.text_on_new_line = text_on_new_line
        self.clock_format_for_intra_day = clock_format_for_intra_day

    def write(self):
        raise NotImplementedError()


class BaseSectionWriter(BaseWriter):
    model = None
    header_writer_class = None
    child_writer_class = None
    trailer_writer_class = None

    def write(self):
        if not self.obj.header or not self.obj.trailer:
            raise ValueError(f"{self.obj.__class__.__name__} missing header or trailer.")

        header = self.header_writer_class(
            self.obj.header,
            line_length=self.line_length,
            text_on_new_line=self.text_on_new_line,
            clock_format_for_intra_day=self.clock_format_for_intra_day
        ).write() if self.obj.header else []

        children = []
        for child in self.obj.children:
            children += self.child_writer_class(
                child,
                line_length=self.line_length,
                text_on_new_line=self.text_on_new_line,
                clock_format_for_intra_day=self.clock_format_for_intra_day
            ).write()

        self.obj.update_totals()
        self.obj.trailer.number_of_records = len(header) + len(children) + 1

        trailer = self.trailer_writer_class(
            self.obj.trailer,
            line_length=self.line_length,
            text_on_new_line=self.text_on_new_line,
            clock_format_for_intra_day=self.clock_format_for_intra_day
        ).write() if self.obj.trailer else []

        return header + children + trailer


class BaseSingleWriter(BaseWriter):
    model = None

    def _write_field_from_config(self, field_config):
        if isinstance(field_config, str):
            field_config = (field_config, lambda w, x: x)

        field_name, write_func = field_config
        field_value = getattr(self.obj, field_name, None)

        if field_value is not None:
            output = write_func(self, field_value)
            if isinstance(output, dict):
                return output
            else:
                return {field_name: convert_to_string(output)}
        else:
            return {field_name: ''}

    def _write_fields_from_config(self, fields_config):
        fields = OrderedDict()
        for field_config in fields_config:
            fields.update(self._write_field_from_config(field_config))
        return fields

    def write(self):
        record = ''
        fields = self._write_fields_from_config(self.fields_config)
        record += self.model.code.value

        for field_name in fields:
            record += ',' + fields[field_name]

        record += '/'
        return [record]


def expand_availability(writer, availability):
    fields = OrderedDict()

    if not availability or len(availability) == 0:
        pass
    elif list(availability.keys()) in [['0', '1', '>1'], ['date', 'time']]:
        for field, value in availability.items():
            if field == 'date':
                value = write_date(value) if value else None
            elif field == 'time':
                value = (write_time(value, writer.clock_format_for_intra_day)
                        if value else None)
            fields[field] = convert_to_string(value) if value is not None else ''
    else:
        fields['distribution_length'] = str(len(availability))
        for field, value in availability.items():
            fields['day_%s' % str(field)] = convert_to_string(field)
            fields['amount_%s' % str(field)] = convert_to_string(value)

    return fields


class TransactionDetailWriter(BaseSingleWriter):
    model = TransactionDetail
    fields_config = [
        ('type_code', lambda w, tc: tc.code if tc else None),
        'amount',
        ('funds_type', lambda w, ft: ft.value if ft else None),
        ('availability', expand_availability),
        'bank_reference',
        'customer_reference',
        'text',
    ]

    def write(self):
        records = ['']
        i = 0

        for field_config in self.fields_config:
            if isinstance(field_config, tuple):
                name, func = field_config
                value = func(self, getattr(self.obj, name, None))
            else:
                value = convert_to_string(getattr(self.obj, field_config, ''))

            # Text continuation logic
            if field_config == 'text' and value:
                text_cursor = 0
                while text_cursor < len(value):
                    remaining = self.line_length - len(records[i]) - 1
                    if remaining <= 0:
                        records.append(CONTINUATION_CODE)
                        i += 1
                        remaining = self.line_length - len(records[i]) - 1
                    end = text_cursor + remaining
                    records[i] += ',' + value[text_cursor:end]
                    text_cursor = end
            else:
                if records[i] != '':
                    records[i] += ','
                records[i] += value

        records[i] += '/'
        return records


def expand_summary_items(writer, summary_items):
    items = OrderedDict()
    if not summary_items:
        return items

    for n, item in enumerate(summary_items):
        for field_config in AccountIdentifierWriter.summary_fields_config:
            if isinstance(field_config, str):
                field_config = (field_config, lambda w, x: convert_to_string(x))
            name, func = field_config
            value = func(writer, getattr(item, name, None))
            items[f"{name}_{n}"] = value if value is not None else ''
    return items


class AccountIdentifierWriter(BaseSingleWriter):
    model = AccountIdentifier
    fields_config = [
        'customer_account_number',
        ('summary_items', expand_summary_items),
    ]

    summary_fields_config = [
        ('type_code', lambda w, tc: tc.code if tc else ''),
        ('amount', lambda w, a: convert_to_string(a)),
        ('item_count', lambda w, c: convert_to_string(c)),
        ('funds_type', lambda w, ft: ft.value if ft else ''),
        ('availability', expand_availability),
    ]

    def write(self):
        records = ['']
        i = 0
        fields = self._write_fields_from_config(self.fields_config)

        for field_name, value in fields.items():
            while value:
                remaining = self.line_length - len(records[i]) - 2
                if remaining <= 0:
                    records.append(CONTINUATION_CODE)
                    i += 1
                    remaining = self.line_length - len(records[i]) - 2
                chunk = value[:remaining]
                if records[i] != '':
                    records[i] += ','
                records[i] += chunk
                value = value[remaining:]

        records[i] += '/'
        return records


class AccountTrailerWriter(BaseSingleWriter):
    model = AccountTrailer
    fields_config = [
        'account_control_total',
        'number_of_records',
    ]


class AccountWriter(BaseSectionWriter):
    model = Account
    header_writer_class = AccountIdentifierWriter
    trailer_writer_class = AccountTrailerWriter
    child_writer_class = TransactionDetailWriter


class GroupHeaderWriter(BaseSingleWriter):
    model = GroupHeader
    fields_config = [
        'ultimate_receiver_id',
        'originator_id',
        ('group_status', lambda w, gs: gs.value if gs else None),
        ('as_of_date', lambda w, d: write_date(d) if d else None),
        ('as_of_time', lambda w, t: write_time(t, w.clock_format_for_intra_day) if t else None),
        # currency field ignored as requested
        ('as_of_date_modifier', lambda w, aodm: aodm.value if aodm else None),
    ]


class GroupTrailerWriter(BaseSingleWriter):
    model = GroupTrailer
    fields_config = [
        'group_control_total',
        'number_of_accounts',
        'number_of_records',
    ]


class GroupWriter(BaseSectionWriter):
    model = Group
    header_writer_class = GroupHeaderWriter
    trailer_writer_class = GroupTrailerWriter
    child_writer_class = AccountWriter


class Bai2FileHeaderWriter(BaseSingleWriter):
    model = Bai2FileHeader
    fields_config = (
        'sender_id',
        'receiver_id',
        ('creation_date', lambda w, d: write_date(d) if d else None),
        ('creation_time', lambda w, t: write_time(t, w.clock_format_for_intra_day) if t else None),
        'file_id',
        'physical_record_length',
        'block_size',
        'version_number',
    )


class Bai2FileTrailerWriter(BaseSingleWriter):
    model = Bai2FileTrailer
    fields_config = (
        'file_control_total',
        'number_of_groups',
        'number_of_records',
    )


class Bai2FileWriter(BaseSectionWriter):
    model = Bai2File
    header_writer_class = Bai2FileHeaderWriter
    trailer_writer_class = Bai2FileTrailerWriter
    child_writer_class = GroupWriter
