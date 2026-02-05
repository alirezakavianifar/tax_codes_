def to_excel(df, file_name):
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        worksheet.right_to_left()
        workbook = writer.book
        number_format = workbook.add_format({'num_format': '#,##0'})
        border_format = workbook.add_format({'border': 1})

        # Apply number format to numeric columns
        numeric_columns = [i for i, col in enumerate(
            df.columns) if col not in ['شماره کارمند', 'اداره']]
        for col_num in numeric_columns:
            worksheet.set_column(col_num, col_num, None, number_format)

        # Apply border to all cells and handle NaN/inf values
        for row_num in range(df.shape[0] + 1):
            for col_num in range(df.shape[1]):
                if row_num == 0:
                    value = df.columns[col_num]
                else:
                    value = df.iloc[row_num - 1, col_num]

                    # Check if the value is a number
                    if isinstance(value, (int, float)):
                        if pd.isna(value) or np.isinf(value):
                            value = ''  # Replace NaN/inf with empty string or any placeholder
                    else:
                        # Ensure non-numeric values are converted to string
                        value = str(value)

                worksheet.write(row_num, col_num, value, border_format)

    return file_name
